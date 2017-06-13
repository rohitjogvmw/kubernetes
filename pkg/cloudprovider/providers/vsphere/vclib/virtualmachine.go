package vclib

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/golang/glog"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
)

// VirtualMachine extends the govmomi VirtualMachine object
type VirtualMachine struct {
	*object.VirtualMachine
}

// IsDiskAttached checks if disk is attached to the VM.
func (vm VirtualMachine) IsDiskAttached(ctx context.Context, diskPath string) (bool, error) {
	// Get object key of controller to which disk is attached
	_, err := vm.GetVirtualDiskControllerKey(ctx, diskPath)
	if err != nil {
		if err == ErrNoDevicesFound {
			return false, nil
		}
		glog.Errorf("Failed to check whether disk is attached. err: %s", err)
		return false, err
	}
	return true, nil
}

// GetVirtualDiskUUIDByPath gets the virtual disk UUID by datastore (namespace) path
//
// volPath can be namespace path (e.g. "[vsanDatastore] volumes/test.vmdk") or
// uuid path (e.g. "[vsanDatastore] 59427457-6c5a-a917-7997-0200103eedbc/test.vmdk").
// `volumes` in this case would be a symlink to
// `59427457-6c5a-a917-7997-0200103eedbc`.
//
// We want users to use namespace path. It is good for attaching the disk,
// but for detaching the API requires uuid path.  Hence, to detach the right
// device we have to convert the namespace path to uuid path.
func (vm VirtualMachine) GetVirtualDiskUUIDByPath(ctx context.Context, diskPath string) (string, error) {
	if len(diskPath) > 0 && filepath.Ext(diskPath) != ".vmdk" {
		diskPath += ".vmdk"
	}
	vdm := object.NewVirtualDiskManager(vm.Client())
	// Returns uuid of vmdk virtual disk
	diskUUID, err := vdm.QueryVirtualDiskUuid(ctx, diskPath, nil)

	if err != nil {
		glog.Errorf("QueryVirtualDiskUuid failed for diskPath: %q, err: %+v", diskPath, err)
		return "", ErrNoDiskUUIDFound
	}
	diskUUID = formatVirtualDiskUUID(diskUUID)
	return diskUUID, nil
}

func getVirtualDiskUUIDByDevice(newDevice types.BaseVirtualDevice) (string, error) {
	virtualDevice := newDevice.GetVirtualDevice()
	if backing, ok := virtualDevice.Backing.(*types.VirtualDiskFlatVer2BackingInfo); ok {
		uuid := formatVirtualDiskUUID(backing.Uuid)
		return uuid, nil
	}
	return "", ErrNoDiskUUIDFound
}

func (vm VirtualMachine) getVirtualDeviceByPath(ctx context.Context, vmDevices object.VirtualDeviceList, diskPath string) (types.BaseVirtualDevice, error) {
	volumeUUID, err := vm.GetVirtualDiskUUIDByPath(ctx, diskPath)
	if err != nil {
		glog.Errorf("Failed to get disk UUID for path: %q. err: %+v", diskPath, err)
		return nil, err
	}
	// filter vm devices to retrieve device for the given vmdk file identified by disk path
	for _, device := range vmDevices {
		if vmDevices.TypeName(device) == "VirtualDisk" {
			diskUUID, _ := getVirtualDiskUUIDByDevice(device)
			if diskUUID == volumeUUID {
				return device, nil
			}
		}
	}
	return nil, nil
}

// GetVirtualDiskControllerKey gets the object key that denotes the controller object to which vmdk is attached.
func (vm VirtualMachine) GetVirtualDiskControllerKey(ctx context.Context, diskPath string) (int32, error) {
	vmDevices, err := vm.Device(ctx)
	if err != nil {
		glog.Errorf("Failed to get the devices for vm: %+v. err: %+v", vm, err)
		return -1, err
	}
	device, err := vm.getVirtualDeviceByPath(ctx, vmDevices, diskPath)
	if err != nil {
		glog.Errorf("Failed to get virtualDevice for path: %q. err: %+v", diskPath, err)
		return -1, err
	} else if device != nil {
		return device.GetVirtualDevice().ControllerKey, nil
	}
	return -1, ErrNoDevicesFound
}

// GetVirtualDiskID gets a device ID which is internal vSphere API identifier for the attached virtual disk.
func (vm VirtualMachine) GetVirtualDiskID(ctx context.Context, diskPath string) (string, error) {
	vmDevices, err := vm.Device(ctx)
	if err != nil {
		glog.Errorf("Failed to get the devices for vm: %+v. err: %+v", vm, err)
		return "", err
	}
	device, err := vm.getVirtualDeviceByPath(ctx, vmDevices, diskPath)
	if err != nil {
		glog.Errorf("Failed to get virtualDevice for path: %q. err: %+v", diskPath, err)
		return "", err
	} else if device != nil {
		return vmDevices.Name(device), nil
	}
	return "", ErrNoDiskIDFound
}

// DeleteVM deletes the VM.
func (vm VirtualMachine) DeleteVM(ctx context.Context) error {
	destroyTask, err := vm.Destroy(ctx)
	if err != nil {
		glog.Errorf("Failed to delete the VM: %+v. err: %+v", vm, err)
		return err
	}
	return destroyTask.Wait(ctx)
}

// AttachDisk attaches the disk at location - vmDiskPath to the Virtual Machine
// Additionally the disk can be configured with SPBM policy if storagePolicyID is non-empty.
func (vm VirtualMachine) AttachDisk(ctx context.Context, vmDiskPath string, storagePolicyID string, diskControllerType string) (diskUUID string, err error) {
	var newSCSIController types.BaseVirtualDevice
	// Check if the diskControllerType is valid
	if !CheckControllerSupported(diskControllerType) {
		return "", fmt.Errorf("Not a valid SCSI Controller Type. Valid options are %q", SCSIControllerTypeValidOptions())
	}
	attached, err := vm.IsDiskAttached(ctx, vmDiskPath)
	if err != nil {
		glog.Errorf("Error occurred while checking if disk is attached. vmDiskPath: %q, err: %+v", vmDiskPath, err)
		return "", err
	}
	if attached {
		diskUUID, _ = vm.GetVirtualDiskUUIDByPath(ctx, vmDiskPath)
		return diskUUID, nil
	}

	disk, newSCSIController, err := createDiskSpec(ctx, vm, vmDiskPath, VolumeOptions{SCSIControllerType: diskControllerType})
	if err != nil {
		glog.Errorf("Error occurred while creating disk spec, err: %v", err)
		return "", err
	}
	virtualMachineConfigSpec := types.VirtualMachineConfigSpec{}
	deviceConfigSpec := &types.VirtualDeviceConfigSpec{
		Device:    disk,
		Operation: types.VirtualDeviceConfigSpecOperationAdd,
	}

	// Configure the disk with the SPBM profile only if ProfileID is not empty.
	if storagePolicyID != "" {
		profileSpec := &types.VirtualMachineDefinedProfileSpec{
			ProfileId: storagePolicyID,
		}
		deviceConfigSpec.Profile = append(deviceConfigSpec.Profile, profileSpec)
	}
	virtualMachineConfigSpec.DeviceChange = append(virtualMachineConfigSpec.DeviceChange, deviceConfigSpec)
	task, err := vm.Reconfigure(ctx, virtualMachineConfigSpec)
	if err != nil {
		glog.Errorf("Failed to attach the disk with storagePolicy: %+q with err - %v", storagePolicyID, err)
		if newSCSIController != nil {
			vm.DeleteController(ctx, newSCSIController)
		}
		return "", "", err
	}
	err = task.Wait(ctx)
	if err != nil {
		glog.Errorf("Failed to attach the disk with storagePolicy: %+q with err - %v", storagePolicyID, err)
		if newSCSIController != nil {
			vm.DeleteController(ctx, newSCSIController)
		}
		return "", "", err
	}

	deviceName, diskUUID, err := vm.GetVMDiskInfo(ctx, disk)
	if err != nil {
		glog.Errorf("Error occurred while getting Disk Info, err: %v", err)
		if newSCSIController != nil {
			vm.DeleteController(ctx, newSCSIController)
		}
		vm.DetachDisk(ctx, vmDiskPath)
		return "", "", err
	}
	return deviceName, diskUUID, nil
}

func (vm VirtualMachine) GetVMDiskInfo(ctx context.Context, disk *types.VirtualDisk) (string, string, error) {
	vmDevices, err := vm.Device(ctx)
	if err != nil {
		glog.Errorf("Error occurred while getting VM devices, err: %v", err)
		return "", "", err
	}
	devices := vmDevices.SelectByType(disk)
	if len(devices) < 1 {
		return "", "", ErrNoDevicesFound
	}

	// get new disk id
	newDevice := devices[len(devices)-1]
	deviceName := devices.Name(newDevice)

	// get device uuid
	diskUUID, err := GetVirtualDiskUUIDByDevice(newDevice)
	if err != nil {
		glog.Errorf("Error occurred while getting Disk UUID of the device, err: %v", err)
		return "", "", err
	}

	return deviceName, diskUUID, nil
}

// DetachDisk detaches the disk specified by vmDiskPath
func (vm VirtualMachine) DetachDisk(ctx context.Context, vmDiskPath string) error {
	vmDevices, err := vm.Device(ctx)
	if err != nil {
		glog.Errorf("Error occurred while getting VM devices, err: %v", err)
		return err
	}
	diskID, err := vm.GetVirtualDiskID(ctx, vmDiskPath)
	if err != nil {
		glog.Errorf("disk ID not found for %v ", vmDiskPath)
		return err
	}
	// Gets virtual disk device
	device := vmDevices.Find(diskID)
	if device == nil {
		glog.Errorf("device '%s' not found", diskID)
		return fmt.Errorf("device '%s' not found", diskID)
	}
	// Detach disk from VM
	err = vm.RemoveDevice(ctx, true, device)
	if err != nil {
		glog.Errorf("Error occurred while removing disk device, err: %v", err)
		return err
	}
	return nil
}

// Get VM's Resource Pool
func (vm VirtualMachine) GetResourcePool(ctx context.Context) (*object.ResourcePool, error) {
	currentVMHost, err := vm.HostSystem(ctx)
	if err != nil {
		glog.Errorf("Failed to get hostsystem for VM, err: %v", err)
		return nil, err
	}
	// Get the resource pool for the current node.
	// We create the dummy VM in the same resource pool as current node.
	resourcePool, err := currentVMHost.ResourcePool(ctx)
	if err != nil {
		glog.Errorf("Failed to get resource pool of the VM, err: %v", err)
		return nil, err
	}
	return resourcePool, nil
}

// Removes latest added SCSI controller from VM.
func (vm VirtualMachine) DeleteController(ctx context.Context, controllerDevice types.BaseVirtualDevice) error {
	if controllerDevice == nil {
		glog.Errorf("Nil value is set for controllerDevice")
		return fmt.Errorf("Nil value is set for controllerDevice")
	}
	if vm.VirtualMachine == nil {
		glog.Errorf("Nil value is set for vm.VirtualMachine")
		return fmt.Errorf("Nil value is set for vm.VirtualMachine")
	}
	// Get VM device list
	vmDevices, err := vm.Device(ctx)
	if err != nil {
		glog.Errorf("Error occurred while getting VM devices, err: %v", err)
		return err
	}
	controllerDeviceList := vmDevices.SelectByType(controllerDevice)
	if len(controllerDeviceList) < 1 {
		return ErrNoDevicesFound
	}
	device := controllerDeviceList[len(controllerDeviceList)-1]
	err = vm.RemoveDevice(ctx, true, device)
	if err != nil {
		glog.Errorf("Error occurred while removing device, err: %v", err)
		return err
	}
	return nil
}

// GetAllAccessibleDatastores gets the list of accessible Datastores for the given Virtual Machine
func (vm VirtualMachine) GetAllAccessibleDatastores(ctx context.Context) ([]Datastore, error) {
	host, err := vm.HostSystem(ctx)
	if err != nil {
		glog.Errorf("Failed to get host system for VM: %+v. err: %+v", vm, err)
		return nil, err
	}
	var hostSystemMo mo.HostSystem
	s := object.NewSearchIndex(vm.Client())
	err = s.Properties(ctx, host.Reference(), []string{DatastoreProperty}, &hostSystemMo)
	if err != nil {
		glog.Errorf("Failed to retrieve datastores for host: %+v. err: %+v", host, err)
		return nil, err
	}
	var dsObjList []Datastore
	for _, dsRef := range hostSystemMo.Datastore {
		dsObjList = append(dsObjList, Datastore{object.NewDatastore(vm.Client(), dsRef)})
	}
	return dsObjList, nil
}

// createAndAttachSCSIController creates and attachs the SCSI controller to the VM.
func (vm VirtualMachine) createAndAttachSCSIController(ctx context.Context, diskControllerType string) (types.BaseVirtualDevice, error) {
	// Get VM device list
	vmDevices, err := vm.Device(ctx)
	if err != nil {
		glog.Errorf("Failed to retrieve VM devices. err: %+v", err)
		return nil, err
	}
	allSCSIControllers := getSCSIControllers(vmDevices)
	if len(allSCSIControllers) >= SCSIControllerLimit {
		// we reached the maximum number of controllers we can attach
		glog.Errorf("SCSI Controller Limit of %d has been reached, cannot create another SCSI controller", SCSIControllerLimit)
		return nil, fmt.Errorf("SCSI Controller Limit of %d has been reached, cannot create another SCSI controller", SCSIControllerLimit)
	}
	newSCSIController, err := vmDevices.CreateSCSIController(diskControllerType)
	if err != nil {
		glog.Errorf("Failed to create new SCSI controller: %+v", err)
		return nil, err
	}
	configNewSCSIController := newSCSIController.(types.BaseVirtualSCSIController).GetVirtualSCSIController()
	hotAndRemove := true
	configNewSCSIController.HotAddRemove = &hotAndRemove
	configNewSCSIController.SharedBus = types.VirtualSCSISharing(types.VirtualSCSISharingNoSharing)

	// add the scsi controller to virtual machine
	err = vm.AddDevice(context.TODO(), newSCSIController)
	if err != nil {
		glog.V(LOG_LEVEL).Infof("Cannot add SCSI controller to vm. err: %+v", err)
		// attempt clean up of scsi controller
		vm.DeleteController(ctx, newSCSIController)
		return nil, err
	}
	return newSCSIController, nil
}

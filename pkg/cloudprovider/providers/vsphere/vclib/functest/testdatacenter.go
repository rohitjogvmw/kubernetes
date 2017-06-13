package main

import (
	"context"
	"fmt"

	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/cloudprovider/providers/vsphere/vclib"
)

const (
	Username       = "Administrator@vsphere.local"
	Password       = "Admin!23"
	VCenterIP      = "10.160.135.171"
	Port           = "443"
	Insecure       = true
	DatacenterName = "vcqaDC"
)

var vSphereConnection = vclib.VSphereConnection{
	Username: Username,
	Password: Password,
	Hostname: VCenterIP,
	Port:     Port,
	Insecure: Insecure,
}

var dc *vclib.Datacenter

func main() {
	err := vSphereConnection.Connect()
	if err != nil {
		glog.Errorf("Failed to connect to VC with err: %v", err)
	}
	fmt.Printf("Successfully connected to VC\n")
	fmt.Printf("===============================================\n")
	if vSphereConnection.GoVmomiClient == nil {
		glog.Errorf("vSphereConnection.GoVmomiClient is not set after a successful connect to VC")
	}

	//Create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dc, err = vclib.GetDatacenter(ctx, vSphereConnection, DatacenterName)

	getVMByUUIDTest(ctx, "423787da-df6c-7306-0518-660397085b6f")
	fmt.Printf("===============================================\n")

	vm1, err := getVMByPath(ctx, "/vcqaDC/vm/kubernetes/master")
	vm2, err := getVMByPath(ctx, "/vcqaDC/vm/kubernetes/node1")
	fmt.Printf("===============================================\n")

	ds1, err := getDatastoreByPath(ctx, "[vsanDatastore] kubevols/redis-master.vmdk")
	fmt.Printf("===============================================\n")

	ds2, err := getDatastoreByName(ctx, "sharedVmfs-0")
	fmt.Printf("===============================================\n")

	getFolderByPath(ctx, "/vcqaDC/vm/kubernetes")
	fmt.Printf("===============================================\n")

	getVMMoList(ctx, []*vclib.VirtualMachine{vm1, vm2}, []string{"name", "summary"})
	fmt.Printf("===============================================\n")

	getDatastoreMoList(ctx, []*vclib.Datastore{ds1, ds2}, []string{"name", "summary"})
	fmt.Printf("===============================================\n")

}

func getVMByUUIDTest(ctx context.Context, vmUUID string) {
	vm, err := dc.GetVMByUUID(ctx, vmUUID)
	if err != nil {
		glog.Errorf("Failed to get VM from vmUUID: %q with err: %v", vmUUID, err)
	}
	fmt.Printf("VM details are %v\n", vm)
}

func getVMByPath(ctx context.Context, vmPath string) (*vclib.VirtualMachine, error) {
	vm, err := dc.GetVMByPath(ctx, vmPath)
	if err != nil {
		glog.Errorf("Failed to get VM from vmPath: %q with err: %v", vmPath, err)
		return nil, err
	}
	fmt.Printf("VM details are %v\n", vm)
	return vm, nil
}

func getDatastoreByPath(ctx context.Context, vmDiskPath string) (*vclib.Datastore, error) {
	ds, err := dc.GetDatastoreByPath(ctx, vmDiskPath)
	if err != nil {
		glog.Errorf("Failed to get Datastore from vmDiskPath: %q with err: %v", vmDiskPath, err)
		return nil, err
	}
	fmt.Printf("Datastore details are %v\n", ds)
	return ds, nil
}

func getDatastoreByName(ctx context.Context, name string) (*vclib.Datastore, error) {
	ds, err := dc.GetDatastoreByName(ctx, name)
	if err != nil {
		glog.Errorf("Failed to get Datastore from name: %q with err: %v", name, err)
		return nil, err
	}
	fmt.Printf("Datastore details are %v\n", ds)
	return ds, nil
}

func getFolderByPath(ctx context.Context, folderPath string) {
	folder, err := dc.GetFolderByPath(ctx, folderPath)
	if err != nil {
		glog.Errorf("Failed to get Datastore from folderPath: %q with err: %v", folderPath, err)
	}
	fmt.Printf("Folder details are %v\n", folder)
}

func getVMMoList(ctx context.Context, vmObjList []*vclib.VirtualMachine, properties []string) {
	vmMoList, err := dc.GetVMMoList(ctx, vmObjList, properties)
	if err != nil {
		glog.Errorf("Failed to get VM managed objects with the given properties from the VM objects. vmObjList: %+v, properties: +%v, err: %+v", vmObjList, properties, err)
	}
	for _, vmMo := range vmMoList {
		fmt.Printf("VM name is %q\n", vmMo.Name)
		fmt.Printf("VM summary is %+v\n", vmMo.Summary)
	}
}

func getDatastoreMoList(ctx context.Context, dsObjList []*vclib.Datastore, properties []string) {
	dsMoList, err := dc.GetDatastoreMoList(ctx, dsObjList, properties)
	if err != nil {
		glog.Errorf("Failed to get datastore managed objects with the given properties from the datastore objects. vmObjList: %+v, properties: +%v, err: %+v", dsObjList, properties, err)
	}
	for _, dsMo := range dsMoList {
		fmt.Printf("Datastore name is %q\n", dsMo.Name)
		fmt.Printf("Datastore summary is %+v\n", dsMo.Summary)
	}
}

#!/usr/bin/env python
# Copyright 2016 VMware, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


'''
ESX-side service  handling VMDK create/attach requests from VMCI clients

The requests (create/delete/attach/detach) are JSON formatted.

All operations are using requester VM (docker host) datastore and
"Name" in request refers to vmdk basename
VMDK name is formed as [vmdatastore] dockvols/"Name".vmdk

Commands ("cmd" in request):
		"create" - create a VMDK in "[vmdatastore] dvol"
		"remove" - remove a VMDK. We assume it's not open, and fail if it is
		"list"   - [future, need docker support] enumerate related VMDK
		"attach" - attach a VMDK to the requesting VM
		"detach" - detach a VMDK from the requesting VM (assuming it's unmounted)

'''

from ctypes import *
import json
import os
import subprocess
import atexit
import time
import logging
import signal
import sys
import re
sys.dont_write_bytecode = True

from vmware import vsi

import pyVim
from pyVim.connect import Connect, Disconnect
from pyVim.invt import GetVmFolder, FindChild
from pyVim import vmconfig

from pyVmomi import VmomiSupport, vim, vmodl

import log_config
import volumeKVStore as kv

# Location of utils used by the plugin.
BinLoc = "/usr/lib/vmware/vmdkops/bin/"

# External tools used by the plugin.
objToolCmd = "/usr/lib/vmware/osfs/bin/objtool open -u "
osfsMkdirCmd = "/usr/lib/vmware/osfs/bin/osfs-mkdir -n "
mkfsCmd = "/usr/lib/vmware/vmdkops/bin/mkfs.ext4 -qF -L "
vmdkCreateCmd = "/sbin/vmkfstools -d thin -c "
vmdkDeleteCmd = "/sbin/vmkfstools -U "

# Defaults
DockVolsDir = "dockvols" # place in the same (with Docker VM) datastore
MaxDescrSize = 10000     # we assume files smaller that that to be descriptor files
MaxJsonSize = 1024 * 4   # max buf size for query json strings. Queries are limited in size
MaxSkipCount = 100       # max retries on VMCI Get Ops failures
DefaultDiskSize = "100mb"

# Service instance provide from connection to local hostd
si = None

# Run executable on ESX as needed for vmkfstools invocation (until normal disk create is written)
# Returns the integer return value and the stdout str on success and integer return value and
# the stderr str on error
def RunCommand(cmd):
   """RunCommand

   Runs command specified by user

   @param command to execute
   """
   logging.debug ("Running cmd %s" % cmd)

   p = subprocess.Popen(cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        shell=True)
   o, e = p.communicate()
   s = p.returncode

   if s != 0:
       return (s, e)

   return (s, o)

# returns error, or None for OK
# opts is  dictionary of {option: value}.
# for now we care about size and (maybe) policy
def createVMDK(vmdkPath, volName, opts=""):
    logging.info ("*** createVMDK: %s opts=%s" % (vmdkPath, opts))
    if os.path.isfile(vmdkPath):
        return err("File %s already exists" % vmdkPath)

    if not opts or not "size" in opts:
        size = DefaultDiskSize
        logging.debug("SETTING DEFAULT SIZE to " +  size)
    else:
        size = str(opts["size"])
        logging.debug("SETTING  SIZE to " + size)

    cmd = "{0} {1} {2}".format(vmdkCreateCmd, size, vmdkPath)
    rc, out = RunCommand(cmd)

    if rc != 0:
        return err("Failed to create %s. %s" % (vmdkPath, out))

    # Create the kv store for the disk before its attached
    ret = kv.create(vmdkPath, "detached", opts)
    if ret != True:
       msg = "Failed to create meta-data store for %s" % vmdkPath
       logging.warning (msg)
       removeVMDK(vmdkPath)
       return err(msg)

    return formatVmdk(vmdkPath, volName)

# Return a backing file path for given vmdk path or none
# if a backing can't be found.
def getVMDKBacking(vmdkPath):
   flatBacking = vmdkPath.replace(".vmdk", "-flat.vmdk")
   if os.path.isfile(flatBacking):
      return flatBacking

   f = open(vmdkPath)
   data = f.read()
   f.close()

   # For now we look for a VSAN URI, later vvol.
   exp = re.compile("RW .* VMFS \"vsan:\/\/(.*)\"")

   try:
      uuid = exp.search(data)
   except:
      return None

   if uuid:
      logging.debug("Got volume UUID %s" % uuid.group(1))
      # Objtool creates a link thats usable to format the
      # vsan object.
      cmd = "{0} {1}".format(objToolCmd, uuid.group(1))
      rc, out = RunCommand(cmd)
      fpath = "/vmfs/devices/vsan/{0}".format(uuid.group(1))
      if rc == 0 and os.path.isfile(fpath):
         return fpath
   return None

def formatVmdk(vmdkPath, volName):
        # Get backing for given vmdk path.
        backing = getVMDKBacking(vmdkPath)

        if backing is None:
           logging.warning ("Failed to format %s." % vmdkPath)
           return err("Failed to format %s." % vmdkPath)

        # Format it as ext4.
        cmd = "{0} {1} {2}".format(mkfsCmd, volName, backing)
        rc, out = RunCommand(cmd)

        if rc != 0:
            logging.warning ("Failed to format %s. %s" % (vmdkPath, out))
            if removeVMDK(vmdkPath) == None:
                return err("Failed to format %s." % vmdkPath)
            else:
                return err("Unable to format %s and unable to delete volume. Please delete it manually." % vmdkPath)
	return None

#returns error, or None for OK
def removeVMDK(vmdkPath):
	logging.info("*** removeVMDK: " + vmdkPath)
        cmd = "{0} {1}".format(vmdkDeleteCmd, vmdkPath)
        rc, out = RunCommand(cmd)
        if rc != 0:
            return err("Failed to remove %s. %s" % (vmdkPath, out))

	return None

def vmdk_is_a_descriptor(filepath):
    """
    Is the file a vmdk descriptor file?  We assume any file that ends in .vmdk
    and has a size less than MaxDescrSize is a desciptor file.
    """
    if filepath.endswith('.vmdk') and os.stat(filepath).st_size < MaxDescrSize:
        try:
            with open(filepath) as f:
                line = f.readline()
                return line.startswith('# Disk DescriptorFile')
        except:
            logging.warning("Failed to open {0} for descriptor check".format(filepath))

    return False

def strip_vmdk_extension(filename):
    """ Remove the .vmdk file extension from a string """
    return filename.replace(".vmdk", "")

# returns a list of volume names (note: may be an empty list)
def listVMDK(path):
	vmdks = [x for x in os.listdir(path) if vmdk_is_a_descriptor(os.path.join(path, x))]
        return [{u'Name': strip_vmdk_extension(x), u'Attributes': {}} for x in vmdks]

# Find VM , reconnect if needed. throws on error
def findVmByName(vmName):
	vm = None
	try:
		vm = FindChild(GetVmFolder(), vmName)
	except vim.fault.NotAuthenticated as ex:
		connectLocal() #  retry
		vm = FindChild(GetVmFolder(), vmName)

	if not vm:
		raise Exception("VM" + vmName + "not found")

	return vm

#returns error, or None for OK
def attachVMDK(vmdkPath, vmName):
	vm = findVmByName(vmName)
	logging.info ("*** attachVMDK: " + vmdkPath + " to "   + vmName +
                  " uuid=" + vm.config.uuid)
	return disk_attach(vmdkPath, vm)

#returns error, or None for OK
def detachVMDK(vmdkPath, vmName):
	vm = findVmByName(vmName)
	logging.info("*** detachVMDK: " + vmdkPath + " from "  + vmName +
                 " VM uuid=" + vm.config.uuid)
	return disk_detach(vmdkPath, vm)


# Check existence (and creates if needed) the path
def getVolPath(vmConfigPath):
    # The volumes folder is created in the parent of the given VM's folder.
    path = os.path.join(os.path.dirname(os.path.dirname(vmConfigPath)), DockVolsDir)

    if os.path.isdir(path):
       # If the path exists then return it as is.
       logging.debug("Found %s, returning" % path)
       return path

    # The osfs tools are usable for all datastores
    cmd = "{0} {1}".format(osfsMkdirCmd, path)
    rc, out = RunCommand(cmd)
    if rc != 0:
       logging.warning("Failed to create " + path)
    else:
       logging.info(path +" created")
       return path

    return None

def getVmdkName(path, volName):
    # form full name as <path-to-volumes>/<volname>.vmdk
    return  os.path.join(path, "%s.vmdk" % volName)

# gets the requests, calculates path for volumes, and calls the relevant handler
def executeRequest(vmName, vmId, configPath, cmd, volName, opts):
    # get /vmfs/volumes/<volid> path on ESX:
    path = getVolPath(configPath)

    if path is None:
       return err("Failed initializing volume path {0}".format(path))

    vmdkPath = getVmdkName(path, volName)


    if cmd == "create":
	   return createVMDK(vmdkPath, volName, opts)
    elif cmd == "remove":
        return removeVMDK(vmdkPath)
    elif cmd == "list":
        return listVMDK(path)
    elif cmd == "attach":
        return attachVMDK(vmdkPath, vmName)
    elif cmd == "detach":
        return detachVMDK(vmdkPath, vmName)
    else:
        return err("Unknown command:" + cmd)


def connectLocal():
	'''
	connect and do stuff on local machine
	'''
	global si #

	# Connect to localhost as dcui
	# User "dcui" is a local Admin that does not lose permissions
	# when the host is in lockdown mode.
	si = pyVim.connect.Connect(host='localhost', user='dcui')
	if not si:
		raise SystemExit("Failed to connect to localhost as 'dcui'.")

	atexit.register(pyVim.connect.Disconnect, si)

	# set out ID in context to be used in request - so we'll see it in logs
	reqCtx = VmomiSupport.GetRequestContext()
	reqCtx["realUser"]='dvolplug'
	return si


def findDeviceByPath(vmdkPath, vm):

 	for d in vm.config.hardware.device:
 		if type(d) != vim.vm.device.VirtualDisk:
 			continue

                # Disks of all backing have a backing object with
                # a filename attribute in it. The filename identifies the
                # virtual disk by name and can be used to try a match
                # with the given name. Filename has format like,
                # "[<datastore name>] <parent-directory>/<vmdk-descriptor-name>".
                backingDisk = d.backing.fileName.split(" ")[1]

                # Construct the parent dir and vmdk name, resolving
                # links if any.
                dvolDir = os.path.dirname(vmdkPath)
                realDvolDir = os.path.basename(os.path.realpath(dvolDir))
                virtualDisk = realDvolDir + "/" + os.path.basename(vmdkPath)
		if virtualDisk == backingDisk:
			logging.debug("findDeviceByPath: MATCH: " + backingDisk)
			return d
	return None


def busInfo(unitNumber, busNumber):
    '''Return a dictionary with Unit/Bus for the vmdk (or error)'''
    return {'Unit': str(unitNumber), 'Bus': str(busNumber)}

def setStatusAttached(vmdkPath, uuid):
    '''Sets metadata for vmdkPath to (attached, attachedToVM=uuid'''
    logging.debug("Set status=attached disk={0} VM={1}".format(vmdkPath, uuid))
    volMeta = kv.getAll(vmdkPath)
    if not volMeta:
        volMeta = []
    volMeta['status'] = 'attached'
    volMeta['attachedVMUuid'] = uuid
    if not kv.setAll(vmdkPath, volMeta):
        logging.warning("Attach: Failed to save Disk metadata", vmdkPath)

def setStatusDetached(vmdkPath):
    '''Sets metadata for vmdkPath to "detached"'''
    logging.debug("Set status=detached disk={0}".format(vmdkPath))
    volMeta = kv.getAll(vmdkPath)
    if not volMeta:
        volMeta = []
    volMeta['status'] = 'detached'
    if 'attachedVMUuid' in volMeta:
        del volMeta['attachedVMUuid']
    if not kv.setAll(vmdkPath, volMeta):
        logging.warning("Detach: Failed to save Disk metadata", vmdkPath)

def getStatusAttached(vmdkPath):
    '''Returns (attached, uuid) tuple. For 'detached' status uuid is None'''

    volMeta = kv.getAll(vmdkPath)
    if not volMeta or 'status' not in volMeta:
        return False, None
    attached = (volMeta['status'] == "attached")
    try:
        uuid = volMeta['attachedVMUuid']
    except:
        uuid = None
    return attached, uuid


def disk_attach(vmdkPath, vm):
  '''
Attaches *existing* disk to a vm on a PVSCI controller
(we need PVSCSI to avoid SCSI rescans in the guest)
return error or unit:bus numbers of newly attached disk.
'''

  # NOTE: vSphere is very picky about unitNumbers and controllers of virtual
  # disks. Every controller supports 15 virtual disks, and the unit
  # numbers need to be unique within the controller and range from
  # 0 to 15 with 7 being reserved (for older SCSI controllers).
  # It is up to the API client to add controllers as needed.
  # SCSI Controller keys are in the range of 1000 to 1003 (1000 + busNumber).
  offset_from_bus_number = 1000
  max_scsi_controllers = 4

  # changes spec content goes here
  dev_changes = []

  devices = vm.config.hardware.device

  # Make sure we have a PVSCI and add it if we don't
  # TODO: add more controllers if we are out of slots. Issue #38

  # get all scsi controllers (pvsci, lsi logic, whatever)
  controllers = [d for d in devices if isinstance(d, vim.VirtualSCSIController)]

  # check if we already have a pvsci one
  pvsci = [d for d in controllers if type(d) == vim.ParaVirtualSCSIController]
  if len(pvsci) > 0:
    diskSlot = None  # need to find out
    controllerKey = pvsci[0].key
    busNumber = pvsci[0].busNumber
  else:
    logging.warning("Warning: PVSCI adapter is missing - trying to add one...")
    diskSlot = 0  # starting on a fresh controller
    if len(controllers) >= max_scsi_controllers:
      msg = "Failed to place PVSCI adapter - out of bus slots"
      logging.error(msg + " VM={0}".format(vm.config.uuid))
      return err(msg)

    # find empty bus slot for the controller:
    taken = set([c.busNumber for c in controllers])
    avail = set(range(0, max_scsi_controllers)) - taken

    key = avail.pop() # bus slot
    controllerKey = key + offset_from_bus_number
    diskSlot = 0
    busNumber = key
    controller_spec = vim.VirtualDeviceConfigSpec(
      operation = 'add',
      device = vim.ParaVirtualSCSIController(
        key = controllerKey,
        busNumber = key,
        sharedBus = 'noSharing',
      ),
    )
    dev_changes.append(controller_spec)

  # Check if this disk is already attached, and if it is - skip the attach
  device = findDeviceByPath(vmdkPath, vm)
  if device:
    # Disk is already attached.
    logging.warning("Disk {0} already attached. VM={1}".format(vmdkPath, vm.config.uuid))
    setStatusAttached(vmdkPath, vm.config.uuid)
    return busInfo(device.unitNumber, device.controllerKey - offset_from_bus_number)

  # Find a slot on the controller, issue attach task and wait for completion
  if not diskSlot:
    taken = set([dev.unitNumber for dev in devices
                 if type(dev) == vim.VirtualDisk and dev.controllerKey == controllerKey])
    # search in 15 slots, with unit_number 7 reserved for scsi controller
    availSlots = set(range (0,6) + range (8,16))  - taken

    if len(availSlots) == 0:
      msg = "Failed to place new disk - out of disk slots"
      logging.error(msg + " VM={0}".format(vm.config.uuid))
      return err(msg)

    diskSlot = availSlots.pop()
    logging.debug(" controllerKey=%d slot=%d" % (controllerKey, diskSlot))
  # add disk here
  disk_spec = vim.VirtualDeviceConfigSpec(
    operation = 'add',
    device = vim.VirtualDisk(
      backing = vim.VirtualDiskFlatVer2BackingInfo(
        fileName = "[] " + vmdkPath ,
        diskMode = 'persistent',
      ),
      deviceInfo = vim.Description(
        # TODO: use docker volume name here. Issue #292
        label = "dockerDataVolume",
        summary = "dockerDataVolume",
      ),
      unitNumber = diskSlot,
      controllerKey = controllerKey,
    ),
  )
  dev_changes.append(disk_spec)

  spec = vim.vm.ConfigSpec()
  spec.deviceChange = dev_changes

  try:
      wait_for_tasks(si, [vm.ReconfigVM_Task(spec=spec)])
  except vim.fault.VimFault as ex:
      msg = ex.msg
      # Use metadata (KV) for extra logging
      kvStatusAttached, kvUuid = getStatusAttached(vmdkPath)
      if kvStatusAttached and kvUuid != vm.config.uuid:
          # KV  claims we are attached to a different VM'.
          msg += " disk {0} already attached to VM={1}".format(vmdkPath, kvUuid)
      return err(msg)

  setStatusAttached(vmdkPath, vm.config.uuid)
  logging.info("Disk %s successfully attached. diskSlot=%d, busNumber=%d" %
               (vmdkPath, diskSlot, busNumber))
  return busInfo(diskSlot, busNumber)


def err(string):
    return {u'Error': string}

def disk_detach(vmdkPath, vm):
  """detach disk (by full path) from a vm amd return None or err(msg)"""

  device = findDeviceByPath(vmdkPath, vm)

  if not device:
    # Could happen if the disk attached to a different VM - attach fails
    # and docker will insist to sending "unmount/detach" which also fails.
    msg = "*** Detach failed: disk={0} not found. VM={1}".format(vmdkPath, vm.config.uuid)
    logging.warning(msg)
    return err(msg)

  spec = vim.vm.ConfigSpec()
  dev_changes = []

  disk_spec = vim.vm.device.VirtualDeviceSpec()
  disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.remove
  disk_spec.device = device
  dev_changes.append(disk_spec)
  spec.deviceChange = dev_changes

  try:
     wait_for_tasks(si, [vm.ReconfigVM_Task(spec=spec)])
  except vim.fault.GenericVmConfigFault as ex:
     for f in ex.faultMessage:
        logging.warning(f.message)
     return err("Failed to detach " + vmdkPath)

  setStatusDetached(vmdkPath)
  logging.info("Disk detached " + vmdkPath)
  return None



def signal_handler_stop(signalnum, frame):
    logging.warn("Received stop signal num: " + `signalnum`)
    sys.exit(0)


# load VMCI shared lib , listen on vSocket in main loop, handle requests
def handleVmciRequests():
	# Load and use DLL with vsocket shim to listen for docker requests
	lib = cdll.LoadLibrary(BinLoc + "/libvmci_srv.so")

	bsize = MaxJsonSize
	txt = create_string_buffer(bsize)

	cartel = c_int32()
	sock = lib.vmci_init()
	skipCount = MaxSkipCount # retries for vmci_get_one_op failures
	while True:
		c = lib.vmci_get_one_op(sock, byref(cartel), txt, c_int(bsize))
		logging.debug("lib.vmci_get_one_op returns %d, buffer '%s'" %(c, txt.value))

		if c == -1:
			# VMCI Get Ops can self-correct by reoping sockets internally. Give it a chance.
			logging.warning("VMCI Get Ops failed - ignoring and moving on.")
			skipCount = skipCount - 1
			if skipCount <= 0:
				raise Exception("Too many errors from VMCI Get Ops - giving up.")
			continue
		else:
			skipCount = MaxSkipCount # reset the counter, just in case

		# Get VM name & ID from VSI (we only get cartelID from vmci, need to convert)
		vmmLeader = vsi.get("/userworld/cartel/%s/vmmLeader" % str(cartel.value))
		groupInfo = vsi.get("/vm/%s/vmmGroupInfo" % vmmLeader)

		# vmId - get and convert to format understood by vmodl as a VM key
		# end result should be like this 564d6865-2f33-29ad-6feb-87ea38f9083b"
		# see KB http://kb.vmware.com/selfservice/microsites/search.do?language=en_US&cmd=displayKC&externalId=1880
		s = groupInfo["uuid"]
		vmId   = "{0}-{1}-{2}-{3}-{4}".format(s[0:8], s[9:12], s[12:16], s[16:20], s[20:32])
		vmName = groupInfo["displayName"]
		cfgPath = groupInfo["cfgPath"]

		try:
			req = json.loads(txt.value, "utf-8")
		except ValueError as e:
			ret = {u'Error': "Failed to parse json '%s'." % (txt,value)}
		else:
			details = req["details"]
			opts = details["Opts"] if "Opts" in details else None
			ret = executeRequest(vmName, vmId, cfgPath, req["cmd"], details["Name"], opts)
			logging.debug("executeRequest ret = %s" % ret)

		err = lib.vmci_reply(c, c_char_p(json.dumps(ret)))
		logging.debug("lib.vmci_reply: VMCI replied with errcode %s " % err)

	lib.close(sock) # close listening socket when the loop is over


def main():
    log_config.configure()
    logging.info("=== Starting vmdkops service ===")

    signal.signal(signal.SIGINT, signal_handler_stop)
    signal.signal(signal.SIGTERM, signal_handler_stop)

    try:
        kv.init()
        connectLocal()
        handleVmciRequests()
    except Exception, e:
    	logging.exception(e)

def getTaskList(propCollector, tasks):
    # Create filter
    obj_specs = [vmodl.query.PropertyCollector.ObjectSpec(obj=task)
                 for task in tasks]
    property_spec = vmodl.query.PropertyCollector.PropertySpec(type=vim.Task,
                                                               pathSet=[],
                                                               all=True)
    filter_spec = vmodl.query.PropertyCollector.FilterSpec()
    filter_spec.objectSet = obj_specs
    filter_spec.propSet = [property_spec]
    return propCollector.CreateFilter(filter_spec, True)

#-----------------------------------------------------------
#
# Support for 'wait for task completion'
# Keep it here to keep a single file for now
#
"""
Written by Michael Rice <michael@michaelrice.org>

Github: https://github.com/michaelrice
Website: https://michaelrice.github.io/
Blog: http://www.errr-online.com/
This code has been released under the terms of the Apache 2 licenses
http://www.apache.org/licenses/LICENSE-2.0.html

Helper module for task operations.
"""

def wait_for_tasks(service_instance, tasks):
    """Given the service instance si and tasks, it returns after all the
   tasks are complete
   """
    task_list = [str(task) for task in tasks]
    property_collector = service_instance.content.propertyCollector
    try:
       pcfilter = getTaskList(property_collector, tasks)
    except vim.fault.NotAuthenticated:
       # Reconnect and retry
       logging.warning ("Reconnecting and retry")
       connectLocal()
       property_collector = si.content.propertyCollector
       pcfilter = getTaskList(property_collector, tasks)

    try:
        version, state = None, None
        # Loop looking for updates till the state moves to a completed state.
        while len(task_list):
            update = property_collector.WaitForUpdates(version)
            for filter_set in update.filterSet:
                for obj_set in filter_set.objectSet:
                    task = obj_set.obj
                    for change in obj_set.changeSet:
                        if change.name == 'info':
                            state = change.val.state
                        elif change.name == 'info.state':
                            state = change.val
                        else:
                            continue

                        if not str(task) in task_list:
                            continue

                        if state == vim.TaskInfo.State.success:
                            # Remove task from taskList
                            task_list.remove(str(task))
                        elif state == vim.TaskInfo.State.error:
                            raise task.info.error
            # Move to next version
            version = update.version
    finally:
        if pcfilter:
            pcfilter.Destroy()

#------------------------



# start the server
if __name__ == "__main__":
    main()

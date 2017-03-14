/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stype "k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/pkg/client/clientset_generated/clientset"
	vsphere "k8s.io/kubernetes/pkg/cloudprovider/providers/vsphere"
	"k8s.io/kubernetes/test/e2e/framework"
)

/*
   Test to verify if VSAN storage capabilities specified in storage-class is being honored while volume creation.
   Valid VSAN storage capabilities are mentioned below:
   1. hostFailuresToTolerate
   2. forceProvisioning
   3. cacheReservation
   4. diskStripes
   5. objectSpaceReservation
   6. iopsLimit

   Steps
   1. Create StorageClass with VSAN storage capabilities set to valid values.
   2. Create PVC which uses the StorageClass created in step 1.
   3. Wait for PV to be provisioned.
   4. Wait for PVC's status to become Bound
   5. Create pod using PVC on specific node.
   6. Wait for Disk to be attached to the node.
   7. Get node VM's devices and find PV's Volume Disk.
   8. Get Backing Info of the Volume Disk and obtain EagerlyScrub and ThinProvisioned
   9. Based on the value of EagerlyScrub and ThinProvisioned, verify diskformat is correct.
   10. Delete pod and Wait for Volume Disk to be detached from the Node.
   11. Delete PVC, PV and Storage Class
*/

var _ = framework.KubeDescribe("VSAN policy support for dynamic provisioning [Volume]", func() {
	f := framework.NewDefaultFramework("volume-vsan-policy")
	var (
		client    clientset.Interface
		namespace string
	)
	BeforeEach(func() {
		framework.SkipUnlessProviderIs("vsphere")
		client = f.ClientSet
		namespace = f.Namespace.Name
		nodeList := framework.GetReadySchedulableNodesOrDie(f.ClientSet)
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
	})

	// Valid policy.
	scParameters := make(map[string]string)
	scParameters["hostFailuresToTolerate"] = "0"
	scParameters["cacheReservation"] = "20"
	framework.Logf("Invoking Test for VSAN storage capabilities: %+v", scParameters)
	It("verify VSAN storage capability hostFailuresToTolerate - 0 and cacheReservation - 20 is honored for dynamically provisioned pv using storageclass", func() {
		By("Invoking Test for diskformat: eagerzeroedthick")
		invokeVSANPolicyTest(client, namespace, scParameters)
	})

	// Invalid policy on a VSAN test bed with 3 ESX hosts.
	scParameters = make(map[string]string)
	scParameters["hostFailuresToTolerate"] = "2"
	scParameters["cacheReservation"] = "20"
	framework.Logf("Invoking Test for VSAN storage capabilities: %+v", scParameters)
	It("verify VSAN storage capability hostFailuresToTolerate - 2 and cacheReservation - 20 is honored for dynamically provisioned pv using storageclass", func() {
		By("Invoking Test for diskformat: eagerzeroedthick")
		invokeVSANPolicyTest(client, namespace, scParameters)
	})

	// Valid policy.
	scParameters = make(map[string]string)
	scParameters["diskStripes"] = "1"
	scParameters["objectSpaceReservation"] = "30"
	framework.Logf("Invoking Test for VSAN storage capabilities: %+v", scParameters)
	It("verify VSAN storage capability diskStripes - 1 and objectSpaceReservation - 30 is honored for dynamically provisioned pv using storageclass", func() {
		By("Invoking Test for diskformat: zeroedthick")
		invokeVSANPolicyTest(client, namespace, scParameters)
	})

	// Valid policy.
	scParameters = make(map[string]string)
	scParameters["objectSpaceReservation"] = "20"
	scParameters["iopsLimit"] = "100"
	framework.Logf("Invoking Test for VSAN storage capabilities: %+v", scParameters)
	It("verify VSAN storage capability objectSpaceReservation - 20 and iopsLimit - 100 is honored for dynamically provisioned pv using storageclass", func() {
		By("Invoking Test for diskformat: thin")
		invokeVSANPolicyTest(client, namespace, scParameters)
	})
})

func invokeVSANPolicyTest(client clientset.Interface, namespace string, scParameters map[string]string) {
	By("Creating Storage Class With VSAN policy params")
	storageClassSpec := getVSphereStorageClassSpec("vsanPolicySC", scParameters)
	storageclass, err := client.StorageV1beta1().StorageClasses().Create(storageClassSpec)
	if err != nil {
		framework.Logf("Failed to create storage class with err: %+v", err)
	}
	Expect(err).NotTo(HaveOccurred())

	defer client.StorageV1beta1().StorageClasses().Delete(storageclass.Name, nil)

	By("Creating PVC using the Storage Class")
	pvclaimSpec := getVSphereClaimSpecWithStorageClassAnnotation(namespace, storageclass)
	pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Create(pvclaimSpec)
	if err != nil {
		framework.Logf("Failed to create PVC with err: %+v", err)
	}
	Expect(err).NotTo(HaveOccurred())

	defer func() {
		client.CoreV1().PersistentVolumeClaims(namespace).Delete(pvclaimSpec.Name, nil)
	}()

	By("Waiting for claim to be in bound phase")
	err = framework.WaitForPersistentVolumeClaimPhase(v1.ClaimBound, client, pvclaim.Namespace, pvclaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
	if err != nil {
		framework.Logf("Failed to bound PVC with err: %+v", err)
	}
	Expect(err).NotTo(HaveOccurred())

	// Get new copy of the claim
	pvclaim, err = client.CoreV1().PersistentVolumeClaims(pvclaim.Namespace).Get(pvclaim.Name, metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred())

	// Get the bound PV
	pv, err := client.CoreV1().PersistentVolumes().Get(pvclaim.Spec.VolumeName, metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred())

	/*
	   PV is required to be attached to the Node. so that using govmomi API we can grab Disk's Backing Info
	   to check EagerlyScrub and ThinProvisioned property
	*/
	By("Creating pod to attach PV to the node")
	// Create pod to attach Volume to Node
	podSpec := getVSpherePodSpecWithClaim(pvclaim.Name, nil, "while true ; do sleep 2 ; done")
	pod, err := client.CoreV1().Pods(namespace).Create(podSpec)
	if err != nil {
		framework.Logf("Failed to create pod spec with err: %+v", err)
	}
	Expect(err).NotTo(HaveOccurred())

	By("Waiting for pod to be running")
	Expect(framework.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)).To(Succeed())

	// get fresh pod info
	pod, err = client.CoreV1().Pods(namespace).Get(pod.Name, metav1.GetOptions{})
	nodeName := pod.Spec.NodeName

	vsp, err := vsphere.GetVSphere()
	Expect(err).NotTo(HaveOccurred())
	verifyVSphereDiskAttached(vsp, pv.Spec.VsphereVolume.VolumePath, k8stype.NodeName(nodeName))

	By("Waiting for pod to be running")
	Expect(framework.WaitForPodNameRunningInNamespace(client, pod.Name, namespace)).To(Succeed())

	By("Delete pod and wait for volume to be detached from node")
	deletePodAndWaitForVolumeToDetach(client, namespace, vsp, nodeName, pod, pv.Spec.VsphereVolume.VolumePath)

}

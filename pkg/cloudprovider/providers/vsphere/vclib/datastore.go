package vclib

import (
	"github.com/vmware/govmomi/object"
)

// Datastore extends the govmomi Datastore object
type Datastore struct {
	*object.Datastore
	datacenter *Datacenter
}

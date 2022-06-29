package servicediscovery

import "time"

// ServiceDiscovery allows us to find workers for a serviceRole and also to register a worker that belongs to a serviceRole
type ServiceDiscovery interface {
	// RegisterWorker or re-register worker with a heartBeat timestamp that proves the worker is alive and well.
	RegisterWorker(worker ServiceWorker, heartBeatTime time.Time) error
	// ListWorkers for a ServiceRole, earliestLastSeen is used to filter out workers that haven't hearbeat after said time
	ListWorkers(service ServiceRole, earliestLastSeen time.Time) ([]*ServiceWorker, error)
}

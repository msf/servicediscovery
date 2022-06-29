package servicediscovery

import (
	"fmt"
	"time"
)

// ServiceRole specifies a service we run on a specific deployment unit
type ServiceRole struct {
	ServiceName string // name of the service this server belongs to (FooService)
	Role        string // role of this server, ie: apiserver, adminserver, worker
	DeployGroup string // many deployments, ie: dev, prod-us, prod-eu, etc..
}

func (svc *ServiceRole) ServiceId() string {
	return fmt.Sprintf(
		"%v/%v/%v",
		svc.ServiceName,
		svc.Role,
		svc.DeployGroup,
	)
}

// ServiceWorker is an instance that runs a given service
type ServiceWorker struct {
	Service  ServiceRole
	WorkerId string
	LastSeen time.Time
	Endpoint string
}

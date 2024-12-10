// network_util.go
package utils

import (
	"errors"
	"net"
)

func IsConnectionClosed(err error) bool {
	return errors.Is(err, net.ErrClosed)
}

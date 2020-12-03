package elasticache

import (
	"bufio"
	"github.com/mediocregopher/radix/v3"
	"go.uber.org/zap"
	"net"
	"strings"
)

func ClusterNodes(_ *zap.Logger, endpoint string) (radix.ClusterTopo, error) {
	if !strings.Contains(endpoint, ":") {
		endpoint = endpoint + ":6379"
	}
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()

	command := "*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n"
	_, err = conn.Write([]byte(command))
	if err != nil {
		return nil, err
	}

	slots := radix.ClusterTopo{}
	err = slots.UnmarshalRESP(bufio.NewReader(conn))
	return slots, err
}

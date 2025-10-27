package gserver

import (
	"fmt"
	"testing"

	"github.com/jgoldverg/grover/pkg"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
)

func TestCreateUdpPort(t *testing.T) {
	groverServerManager := GroverUdpServer{
		UnimplementedGroverServerServer: pb.UnimplementedGroverServerServer{},
		lm:                              pkg.NewListenerManager(),
		handler:                         nil,
		mtuPort:                         0,
	}
	req := pb.CreateUdpPortsRequest{PortCount: 0}

	resp, err := groverServerManager.CreateUdpPorts(t.Context(), &req)
	if err != nil {
		t.Errorf("CreateUdpPorts err: %v", err)
	}
	if len(resp.Ports) != 0 {
		t.Errorf("CreateUdpPorts err: %v", resp.Ports)
	}

	req = pb.CreateUdpPortsRequest{PortCount: 1}
	resp, err = groverServerManager.CreateUdpPorts(t.Context(), &req)
	if err != nil {
		t.Errorf("CreateUdpPorts err: %v", err)
	}
	if len(resp.Ports) != 1 {
		t.Errorf("CreateUdpPorts err: %v", resp.Ports)
	}
	fmt.Println(resp.Ports)
}

func TestDeleteUdpPort(t *testing.T) {
	groverServerManager := GroverUdpServer{
		UnimplementedGroverServerServer: pb.UnimplementedGroverServerServer{},
		lm:                              pkg.NewListenerManager(),
		handler:                         nil,
		mtuPort:                         0,
	}

	createOnePort := pb.CreateUdpPortsRequest{PortCount: 1}
	_, err := groverServerManager.CreateUdpPorts(t.Context(), &createOnePort)
	if err != nil {
		t.Errorf("CreateUdpPorts err: %v", err)
	}
	deleteWrongPort := pb.DeleteUdpPortsRequest{PortNum: []uint32{0}}
	deleteResp, err := groverServerManager.DeleteUdpPorts(t.Context(), &deleteWrongPort)
	if err != nil {
		t.Errorf("DeleteUdpPorts err: %v", err)
	}
	if !deleteResp.GetOk() {
		t.Errorf("DeleteUdpPorts err: %v", deleteResp.GetOk())
	}
}

func TestListPorts(t *testing.T) {
	groverServerManager := GroverUdpServer{
		UnimplementedGroverServerServer: pb.UnimplementedGroverServerServer{},
		lm:                              pkg.NewListenerManager(),
		handler:                         nil,
		mtuPort:                         0,
	}
	req := pb.ListPortRequest{}
	resp, err := groverServerManager.ListPorts(t.Context(), &req)
	if err != nil {
		t.Errorf("ListPorts err: %v", err)
	}
	if len(resp.GetPort()) != 0 {
		t.Errorf("ListPorts should be 0 right now: %v", resp.GetPort())
	}

	cReq := pb.CreateUdpPortsRequest{PortCount: 9}
	cResp, err := groverServerManager.CreateUdpPorts(t.Context(), &cReq)
	if err != nil {
		t.Errorf("failed to create 9 udp ports err: %v", err)
	}
	lReq := pb.ListPortRequest{}
	lResp, err := groverServerManager.ListPorts(t.Context(), &lReq)
	for _, port := range cResp.GetPorts() {
		found := false
		for _, lPort := range lResp.GetPort() {
			if port == lPort {
				found = true
				break
			}
		}
		if found == false {
			t.Errorf("Port %d was not found in the list response: %v", port, lResp.GetPort())
		}
	}
}

func TestStopServer(t *testing.T) {
	groverServerManager := GroverUdpServer{
		UnimplementedGroverServerServer: pb.UnimplementedGroverServerServer{},
		lm:                              pkg.NewListenerManager(),
		handler:                         nil,
		mtuPort:                         0,
	}
	req := pb.CreateUdpPortsRequest{PortCount: 10}
	_, err := groverServerManager.CreateUdpPorts(t.Context(), &req)
	if err != nil {
		t.Errorf("CreateUdpPorts err: %v", err)
	}
	stopServerReq := pb.StopServerRequest{}
	resp, err := groverServerManager.StopServer(t.Context(), &stopServerReq)
	if err != nil {
		t.Errorf("StopServer err: %v", err)
	}
	if !resp.GetOk() {
		t.Errorf("StopServer err: %v", resp.GetOk())
	}
}

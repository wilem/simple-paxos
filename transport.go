package main

import (
	"fmt"
	"log"
	"net"
	"time"
)

//ITransport - interface of tranport class
type ITransport interface {
	Start() error
	Stop() error
	SendTo(int, []byte) (int, error)
}

// OnRecvCallback is a callback type for user to register with.
type OnRecvCallback func(uint32, []byte)

// UDPTransport encapsulate UDP transport
type UDPTransport struct {
	id            uint32
	clientConnMap map[uint32]*net.UDPConn //remoteID -> client connection.
	OnRecv        OnRecvCallback
}

//NewUDPTransport -
func NewUDPTransport(id uint32) *UDPTransport {
	u := new(UDPTransport)
	u.id = id
	u.clientConnMap = make(map[uint32]*net.UDPConn)
	return u
}

//getLocalClientAddress - return local UDP client side address
func (t UDPTransport) getClientAddress(remoteID uint32) string {
	addr := ids2addr(t.id, remoteID)
	return addr
}

func (t UDPTransport) getServerAddress(serverID uint32) string {
	addr := ids2addr(0, serverID)
	return addr
}

//RecvBufSize : recv buffer size used in server recv loop.
const RecvBufSize int = 1024 * 4

//Start : start a UDP server loop
func (t UDPTransport) Start() error {
	//TODO use ids2port()
	addr := t.getServerAddress(t.id)
	fmt.Println("UDP server listen on:", addr)
	laddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatalf("net.ResolveUDPAddr - err:%s, addr:%+v\n", err, addr)
		return err
	}
	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		log.Fatalf("net.ListenUDP - err:%s, laddr:%+v\n", err, laddr)
		return err
	}
	//defer conn.Close() //can not close here!XXX

	buffer := make([]byte, RecvBufSize)
	go func() {
		for {
			n, raddr, err := conn.ReadFromUDP(buffer)
			if err != nil {
				fmt.Println("UDP server read:", buffer[:n],
					"from ", addr, "err:", err)
				time.Sleep(time.Second)
				continue
			}
			src, dst := port2ids(uint32(raddr.Port))
			if dst != t.id {
				log.Printf("[%d]Wrong package received - dst:%d\n", t.id, dst)
				continue
			}

			if t.OnRecv != nil {
				t.OnRecv(src, buffer[:n])
			} else {
				log.Printf("[%d]t.OnRecv: - buffer:%d\n", t.id, buffer[:n])
			}
		}
	}()

	return nil
}

//SendTo - send bytes to remote node.
func (t UDPTransport) SendTo(to uint32, data []byte) (int, error) {
	var conn *net.UDPConn
	var err error
	var ok bool
	conn, ok = t.clientConnMap[to]
	if !ok {
		client := t.getClientAddress(to)
		server := t.getServerAddress(to)
		conn, err = newClientConn(client, server)
		if err != nil {
			fmt.Println("new client conn: failed.")
			return -1, err
		}
		fmt.Printf("new client conn: %+v\n", *conn)
		t.clientConnMap[to] = conn
	}

	return conn.Write(data)
}

//Stop : stop server
func (t *UDPTransport) Stop() error {
	//TODO
	return nil
}

func newClientConn(localAddr, remoteAddr string) (*net.UDPConn, error) {
	laddr, err := net.ResolveUDPAddr("udp", localAddr)
	if err != nil {
		return nil, err
	}

	raddr, err := net.ResolveUDPAddr("udp", remoteAddr)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialUDP("udp", laddr, raddr)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// utilities
func ids2addr(src, dst uint32) string {
	str := fmt.Sprintf("127.0.0.1:5%02d%02d", src, dst)
	return str
}

//LocalIPAddr : local IP address
const LocalIPAddr = "127.0.0.1"

func newUDPAddr(src, dst uint32) *net.UDPAddr {
	addr := net.UDPAddr{
		Port: int(50000 + src*100 + dst),
		IP:   net.ParseIP(LocalIPAddr),
	}
	return &addr
}

func port2ids(port uint32) (src, dst uint32) {
	src, dst = (port-50000)/100, port%100
	return
}

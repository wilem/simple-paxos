package main

import "log"

//Actor : Client/Proposer/Acceptor/Learner
type Actor interface {
	getID() uint32
}

//Client :
type Client struct {
	node *Node
}

//Start :
func (c *Client) Start() error {
	return nil
}

//DefaultLeaderNodeID :
const DefaultLeaderNodeID uint32 = 1

//Submit : send value to proposer, return errno and error.
func (c *Client) Submit(seq uint32, val *Value) (int, error) {
	dst := DefaultLeaderNodeID //c.node.leaderID//default leader node id
	msg := NewPxsMsgRequest(seq, val)
	oct, err := msg.Encode()
	if err != nil {
		return -1, err
	}
	return c.node.SendTo(dst, oct)
}

//OnRecvResponse : on recv response from acceptors
func (c *Client) OnRecvResponse(rsp *PxsMsgResponse, from uint32) (ret int) {
	log.Printf("[%d]Client.OnRecvResponse - rsp:%+v, from:%d, ret:%d\n",
		c.node.id, rsp, from, rsp.ret)
	return 0
}

//Proposer :
type Proposer struct {
	node *Node
	p1a  map[uint32]uint32 //iid -> bal
	//iid -> bal
	//delete this entry after iid is chosen;
	//TODO if p1a msg is TIMEOUT before promised by quorum then incr bal;
	pendingList []*PxsMsgRequest
	//acceptor state
	//maxBal  map[[2]uint32]uint32         //[iid,acc] -> mBal
	//maxVBal map[[2]uint32]uint32         //[iid,acc] -> mVBal
	//maxVal  map[[2]uint32]*Value         //[iid,acc] -> mVal
	p1b       map[[2]uint32]*PxsMsgPromise  //[iid,acc] -> P1b{bal,vbal,val}
	p2a       map[[2]uint32]*PxsMsgAccept   //[iid,bal] -> P2a{bal,val}
	p2b       map[[2]uint32]*PxsMsgAccepted //[iid,acc] -> P2b{bal,val}
	nPromised map[uint32]uint32             //iid -> nPromised
	nAccepted map[uint32]uint32             //iid -> nAccepted
	phase     map[uint32]uint32             //pxsPhaseState
	gotOldVal map[[2]uint32]bool            //[iid,bal] -> gotOldVal; got old value from acceptors
}

//proposer : algorithm phase
const (
	pxsPhaseIdle              uint32 = 0
	pxsPhaseSendPrepare       uint32 = 10
	pxsPhaseQuorumPromised    uint32 = 20
	pxsPhaseSendAccept        uint32 = 30 //send accept
	pxsPhaseSendAcceptNew     uint32 = 31 //send accept with new value from pendingList;
	pxsPhaseSendAcceptOld     uint32 = 32 //send accept with old value from acceptors;
	pxsPhaseQuorumAccepted    uint32 = 40 //accepted
	pxsPhaseQuorumAcceptedNew uint32 = 41 //accepted new value
	pxsPhaseQuorumAcceptedOld uint32 = 42 //accepted old value
	pxsPhaseSendCommit        uint32 = 50 //commit value
	pxsPhaseSendCommitNew     uint32 = 51 //commit new value
	pxsPhaseSendCommitOld     uint32 = 52 //commit old value
	pxsPhaseCommitted         uint32 = 60 //committed to RSM
)

//NewProposer :
func NewProposer(node *Node) *Proposer {
	p := new(Proposer)
	p.node = node
	p.p1a = make(map[uint32]uint32)
	p.p1b = make(map[[2]uint32]*PxsMsgPromise)
	p.p2a = make(map[[2]uint32]*PxsMsgAccept)
	p.p2b = make(map[[2]uint32]*PxsMsgAccepted)
	p.nPromised = make(map[uint32]uint32)
	p.nAccepted = make(map[uint32]uint32)
	p.phase = make(map[uint32]uint32)
	p.gotOldVal = make(map[[2]uint32]bool)
	return p
}

//Start :
func (p *Proposer) Start() error {
	//TODO start election timer
	return nil
}

//getNextBallot : select a ballot number
func (p *Proposer) getNextBallot(iid uint32) uint32 {
	if p.p1a[iid] == 0 {
		p.p1a[iid] = p.node.id * 100
		//1: 100~, 2: 200~; higher node ID wins;
	}
	p.p1a[iid]++
	return p.p1a[iid]
}

//SendToAllAcceptors : send one msg to all acceptors.
func SendToAllAcceptors(node *Node, bs []byte) (ret int) {
	var nok uint32
	for _, id := range node.cfg.AcceptorList {
		nwr, err := node.SendTo(id, bs)
		if nwr == len(bs) {
			nok++
		} else {
			ret = nwr
			log.Printf("[%d]node.SendTo - ret:%d, err:%s\n", node.id, nwr, err)
		}
	}
	//check cluster status
	if nok < node.quorum {
		log.Printf("[%d]node.SendTo - nok:%d, cluster is not ready.\n", node.id, nok)
		return int(PxsStatusClusterUnavailable)
	}
	return 0
}

//OnRecvRequest : client requests.
func (p *Proposer) OnRecvRequest(req *PxsMsgRequest, from uint32) (ret int) {
	log.Printf("[%d]Proposer.OnRecvRequest - req:%+v, from:%d\n", p.node.id, req, from)
	//1. enqueue pending list.
	p.pendingList = append(p.pendingList, req) //dequeue once value is chose
	iid := p.node.instanceID                   //nextInstanceID, XXX update it later.
	bal := p.getNextBallot(iid)                //XXX select a ballot number.
	//2. if it is leader.
	var bs []byte
	if p.node.id == p.node.leaderID {
		//TODO free to send p2a msg directly.
		//once every acceptor accept the proposal,
		//then acceptors should set the leaderID;
		p2a := NewPxsMsgAccept(iid, bal, &p.pendingList[0].val)
		bs, _ = p2a.Encode()

		log.Printf("[%d]Proposer.SendAccept - p2a:%+v\n", p.node.id, p2a)

		ret = SendToAllAcceptors(p.node, bs)
		if ret == 0 {
			p.phase[iid] = pxsPhaseSendAccept
			iidBal := [2]uint32{iid, bal}
			p.p2a[iidBal] = p2a
			log.Printf("[%d]phase2: iidBal:%+v, p2a:%+v\n", p.node.id, iidBal, p2a)
		} else {
			log.Printf("Failed: SendToAllAcceptors\n")
		}
	} else {
		//3. if it is not leader then:
		//need to elect itself first, once quorum accept the proposal, it becomes leader.
		//iid := p.node.instanceID + 1 //nextInstanceID, XXX update it later.
		bal := p.getNextBallot(iid)
		p1a := NewPxsMsgPrepare(iid, bal)
		bs, _ = p1a.Encode()

		log.Printf("[%d]Proposer.SendPrepare - p1a:%+v\n", p.node.id, p1a)

		ret = SendToAllAcceptors(p.node, bs)

		p.phase[iid] = pxsPhaseSendPrepare
	}
	//log.Printf("[%d]SendToAllAcceptors - bs:%+v, ret:%d\n", p.node.id, bs, ret)
	return
}

//OnRecvPromise : from acceptors
func (p *Proposer) OnRecvPromise(pro *PxsMsgPromise, from uint32) (ret int) {
	log.Printf("[%d]Proposer.OnRecvPromise - pro:%+v, acc:%d\n", p.node.id, pro, from)
	iid := pro.hdr.iid
	acc := pro.acc
	bal := pro.bal
	//0. reject unmatch ballot
	if bal != p.p1a[iid] {
		log.Printf("[%d]drop unmatch ballot:%d, acc:%d, iid:%d\n", p.node.id, bal, acc, iid)
		return -1 //XXX
	}
	//1. p1b msg enqueue
	iidAcc := [2]uint32{iid, acc} //XXX
	p.p1b[iidAcc] = pro
	//2. check phase state
	if p.phase[iid] != pxsPhaseSendPrepare &&
		p.phase[iid] != pxsPhaseQuorumPromised { //XXX
		log.Printf("[%d]drop unexpected promise, phase:%d\n", p.node.id, p.phase[iid])
		return -1 //XXX
	}
	//3. check quorum
	var nPromised uint32
	for k := range p.p1b {
		if k[0] == iid {
			nPromised++
		}
	}
	p.nPromised[iid] = nPromised
	if nPromised >= p.node.quorum { //got quorum
		log.Printf("[%d]Proposer - got quorum.", p.node.id)
		if p.phase[iid] == pxsPhaseSendPrepare {
			p.phase[iid] = pxsPhaseQuorumPromised
			return p.Phase2(iid, bal)
		}
	}
	return 0
}

//Phase2 : proposer carry out phase 2, assum phase is promised;
func (p *Proposer) Phase2(iid, bal uint32) (ret int) {
	var maxVBal uint32
	var msg *PxsMsgPromise
	var val *Value

	//1. try to find old value from promises
	for k, m := range p.p1b {
		if k[0] == iid { //XXX
			if m.mVbal != Invalidballot && m.mVbal > maxVBal {
				maxVBal = m.mVbal
				msg = m
			}
		}
	}

	//2. found old value
	if msg != nil {
		p.gotOldVal[[2]uint32{iid, bal}] = true
		val = &msg.mval
		//bal := msg.bal //assert
		log.Printf("[%d]Phase2 - iid:%d, old value:%+v\n", p.node.id, iid, val)
	} else {
		//2.1 send new value
		val = &p.pendingList[0].val
	}

	//3. send accept
	p2a := NewPxsMsgAccept(iid, bal, val)
	bs, _ := p2a.Encode()

	log.Printf("[%d]Proposer.SendAccept: %+v\n", p.node.id, p2a)

	ret = SendToAllAcceptors(p.node, bs)
	if ret == 0 {
		p.phase[iid] = pxsPhaseSendAccept
		iidBal := [2]uint32{iid, bal}
		p.p2a[iidBal] = p2a
		log.Printf("[%d]phase2: iidBal:%+v, p2a:%+v\n", p.node.id, iidBal, p2a)
	} else {
		log.Printf("Failed: SendToAllAcceptors\n")
	}

	return
}

//OnRecvAccepted : from acceptor
func (p *Proposer) OnRecvAccepted(acd *PxsMsgAccepted, from uint32) (ret int) {
	log.Printf("[%d]Proposer.OnRecvAccepted - from:%d, p2b:%+v\n", p.node.id, from, acd)
	iid := acd.hdr.iid
	bal := acd.bal
	acc := acd.acc

	if acc != from {
		log.Printf("[%d]Unmatch acceptor id - acc:%d != from:%d\n", p.node.id, acc, from)
		return -1
	}

	//0. check ballot and value
	iidBal := [2]uint32{iid, bal}
	_, ok := p.p2a[iidBal]
	if !ok {
		log.Printf("[%d]Unmatch p2a - iidBal:%+v\n", p.node.id, iidBal)
		return -1
	}
	//TODO check value: p2a.val == acd.val

	//1. check phase state
	if p.phase[iid] != pxsPhaseSendAccept {
		log.Printf("[%d]Unexpected p2b - iid:%d, bal:%d; phase:%d\n",
			p.node.id, iid, bal, p.phase[iid])
		return -1
	}

	//2. check quorum
	iidAcc := [2]uint32{iid, acc}
	p.p2b[iidAcc] = acd
	var nrsp uint32
	for k := range p.p2b {
		if k[0] == iid {
			nrsp++
		}
	}
	if nrsp >= p.node.quorum {
		if p.phase[iid] == pxsPhaseSendAccept {
			p.phase[iid] = pxsPhaseQuorumAccepted
			log.Printf("[%d]Accepted got quorum - iid:%d,bal:%d,nrsp:%d\n", p.node.id, iid, bal, nrsp)
			//send commit
			cmt := NewPxsMsgCommit(iid, bal)
			bs, _ := cmt.Encode()
			ret = SendToAllAcceptors(p.node, bs)
			if ret == 0 {
				p.phase[iid] = pxsPhaseSendCommit
				//TODO cleanup p1a,p2a,cmt msgs
				//TODO execute RSM
				//TODO check if it is old values
				p.node.instanceID++
				p.runPendingList(true)
			}
		}
	}

	return //ret
}

//runPendingList : clean up and run pendingList;
func (p *Proposer) runPendingList(pop bool) {
	if pop { // 1st element.
		p.pendingList = p.pendingList[1:]
	}
	if len(p.pendingList) > 0 {
		//TODO propose remain values

	}
}

//Acceptor :
type Acceptor struct {
	node    *Node
	maxBal  map[uint32]uint32 //iid -> mBal; highest ballot promised;
	maxVBal map[uint32]uint32 //iid -> mVBal; highet ballot accepted;
	maxVal  map[uint32]*Value //iid -> mVal; value with the maxVBal;
}

//NewAcceptor :
func NewAcceptor(node *Node) *Acceptor {
	a := new(Acceptor)
	a.node = node
	a.maxBal = make(map[uint32]uint32)
	a.maxVBal = make(map[uint32]uint32)
	a.maxVal = make(map[uint32]*Value)
	return a
}

//Start :
func (a *Acceptor) Start() error {
	return nil
}

//OnRecvPrepare : from proposer
func (a *Acceptor) OnRecvPrepare(p1a *PxsMsgPrepare, from uint32) (int, error) {
	//var ret int
	iid := p1a.hdr.iid
	bal := p1a.bal
	//p1b:
	if bal > a.maxBal[iid] { /*maxBal = 0, if not found*/
		a.maxBal[iid] = bal       //promised ballot
		var vbal uint32           //accepted ballot
		var val *Value            //accepted value
		if a.maxVal[iid] != nil { //have old value
			vbal = a.maxVBal[iid] //have voted ballot
			val = a.maxVal[iid]   //have voted value
		} else {
			// without voted value
			vbal = Invalidballot //without voted ballot
			val = &Value{}       //without voted value
		}
		p1b := NewPxsMsgPromise(iid, a.node.id, bal, vbal, val)
		bs, _ := p1b.Encode()
		nwr, err := a.node.SendTo(from, bs)
		if nwr != len(bs) {
			log.Printf("[%d]node.SendTo failed - nwr:%d, len(bs):%d\n",
				a.node.id, nwr, len(bs))
			return int(PxsStatusNetworkIOFailure), err
		}

		return 0, nil
	}
	//drop lower ballot p1a msg.
	log.Printf("[%d]DROPPED msg - p1a:%+v\n, from:%d\n", a.node.id, p1a, from)
	return 0, nil
}

//OnRecvAccept : from proposer
func (a *Acceptor) OnRecvAccept(p2a *PxsMsgAccept, from uint32) (int, error) {
	log.Printf("[%d]Acceptor.OnRecvAccept - p2a:%+v,from:%d\n", a.node.id, p2a, from)
	iid := p2a.hdr.iid
	bal := p2a.bal
	val := &p2a.val
	maxBal := a.maxBal[iid] //maxBal can be 0 if accept it without phase 1;
	if maxBal <= bal {
		//accept proposal
		a.maxBal[iid] = bal
		a.maxVBal[iid] = bal
		a.maxVal[iid] = val
		//send p2b
		p2b := NewPxsMsgAccepted(iid, a.node.id, bal, val)
		bs, _ := p2b.Encode()
		return a.node.SendTo(from, bs)
	}

	log.Printf("[%d]NACK for p2a - maxBal:%d > bal:%d\n", a.node.id, maxBal, bal)
	return 0, nil
}

//OnRecvCommit :
func (a *Acceptor) OnRecvCommit(cmt *PxsMsgCommit, from uint32) (int, error) {
	log.Printf("[%d]Acceptor.OnRecvCommit - cmt:%+v,from:%d\n", a.node.id, cmt, from)
	//TODO
	if a.node.leaderID != from {
		a.node.leaderID = from //update leader ID
		log.Printf("[%d]Leader node ID changed to %d\n", a.node.id, from)
	}
	return 0, nil
}

//Learner :
type Learner struct {
	node *Node
}

//Start :
func (a *Learner) Start() error {
	return nil
}

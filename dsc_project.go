package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

var allVirtualNodes []*virtualNode // global list of virtual nodes
var numOfVirtualNodes int = 5      // assume 5 virtual nodes
var numOfRoomIds int = 10          // assume 10 different rooms
var numOfReplicas int = 3          // assume no replication
const rReplication int = 2
const wReplication int = 3

func main() {

	allVirtualNodes = make([]*virtualNode, numOfVirtualNodes)

	// room id (0 - 9)
	// roomId // numOfVirtualNodes -> [0, 4]

	// create virtual nodes
	vMutex := sync.RWMutex{}
	pMutex := sync.RWMutex{}
	for id := 0; id < numOfVirtualNodes; id++ {
		nodeNew := virtualNode{ch: make(chan message),
			nodeList:     make([]*physicalNode, numOfRoomIds/numOfVirtualNodes*numOfReplicas),
			hashID:       id,
			roomToPos:    make(map[int]int),
			replyCount:   make(map[string]int),
			vnodeMutex:   &vMutex,
			pnodeReplies: make(map[string]*physicalNode),
			pnodeMutex:   &pMutex}

		for j := 0; j < numOfRoomIds/numOfVirtualNodes; j++ {
			for k := 0; k < numOfReplicas; k++ {
				var newPhysicalNode physicalNode
				if k == 0 {
					newPhysicalNode = physicalNode{roomID: (id + k + j*numOfVirtualNodes) % numOfRoomIds, studentID: 0, localLogicalClock: 0, parentVnode: &nodeNew}

					nodeNew.roomToPos[(id+k+j*numOfVirtualNodes)%numOfRoomIds] = j + k*numOfRoomIds/numOfVirtualNodes
				} else {
					newPhysicalNode = physicalNode{roomID: (id + k + (numOfVirtualNodes - numOfReplicas) + j*numOfVirtualNodes) % numOfRoomIds, studentID: 0, localLogicalClock: 0, parentVnode: &nodeNew} //localVectorClock: make([]int, numOfRoomIds

					nodeNew.roomToPos[(id+k+(numOfVirtualNodes-numOfReplicas)+j*numOfVirtualNodes)%numOfRoomIds] = j + k*numOfRoomIds/numOfVirtualNodes
				}
				nodeNew.nodeList[j+k*numOfRoomIds/numOfVirtualNodes] = &newPhysicalNode

			}
			//nodeNew.roomToPos[id+j*numOfVirtualNodes] = j
		}

		go nodeNew.virtualNodeWait()
		allVirtualNodes[id] = &nodeNew
	}
	// checkPhysicalNodes()

	// this to test multiple nodes

	go generalWait(1, "create", 1004123)
	go generalWait(2, "create", 1004999)

	// go generalWait(3, "read", 1004345)
	// go generalWait(2, "read", 1004345)

	// go generalWait(2, "create", 1004999)

	//this takes in input from client from command line

	// var inputStudentID, inputRoomNumber int
	// var inputOperation string

	for {
		// fmt.Println("What is your student ID")
		// fmt.Scanln(&inputStudentID)
		// fmt.Println("Do you want to create or delete a booking?\n create \t delete")
		// fmt.Scanln(&inputOperation)
		// fmt.Println("Enter the room number from 0-9")
		// fmt.Scanln(&inputRoomNumber)
		// go generalWait(inputRoomNumber, inputOperation, inputStudentID)

	}
}

func checkPhysicalNodes() { // for testing purposes
	for _, vnode := range allVirtualNodes {
		fmt.Println("Virtual Node is ", vnode.hashID)
		for _, pnode := range vnode.nodeList {
			fmt.Println("Physical Node is ", pnode.roomID)
		}
		fmt.Println("Room to pos")
		fmt.Println(vnode.roomToPos)

	}

}

func generalWait(roomId int, op string, studentId int) { // op can be "create" or "delete"
	// hash room id
	inputHashId := roomId % numOfVirtualNodes
	//fmt.Println("Entering General wait")
	fmt.Println("Trying to " + op + " room " + strconv.Itoa(roomId) + " by studentID: " + strconv.Itoa(studentId))

	// check which virtual node this hashed value belong to
	for _, vnode := range allVirtualNodes {
		if inputHashId == vnode.hashID {
			// create message
			msgNew := new(message)
			msgNew.msgRoomId = roomId
			msgNew.operation = op
			msgNew.msgStudentId = studentId

			vnodech := vnode.ch

			vnodech <- *msgNew
		}
	}

}

type virtualNode struct {
	ch        chan message
	nodeList  []*physicalNode
	hashID    int
	roomToPos map[int]int // mapping between physical node roomID (key) and its position in nodeList (value)
	// replyCount  map[int]map[int]int // replyCount[roomID][studentID] to get reply count
	replyCount  map[string]int // map[roomID + studentID] to get count
	requestDONE bool

	replicaAliveArr []*virtualNode // create array for replicas

	vnodeMutex   *sync.RWMutex
	pnodeReplies map[string]*physicalNode // map[roomID + studentID] to get pnode replies
	pnodeMutex   *sync.RWMutex

	// requestBoolean bool
}

type message struct {
	msgRoomId    int
	msgStudentId int
	operation    string // "create", "delete"
	sender       *virtualNode
	senderPnode  *physicalNode
}
type physicalNode struct {
	roomID            int
	studentID         int
	localLogicalClock int
	originalPnode     *physicalNode
	ch                chan int
	parentVnode       *virtualNode
}

func (vnode *virtualNode) virtualNodeWait() {
	//fmt.Println("Node " + strconv.Itoa(vnode.hashID) + " is now waiting")
	virtualNodeCh := vnode.ch
	for {

		// will take in input from general function which is the HASHED room id to be querried
		// will then identify the physical node that contains the HASHED room id
		select {
		case msg, ok := <-virtualNodeCh:
			if ok {
				// addPhysicalNode()
				roomPosition := vnode.roomToPos[msg.msgRoomId]
				roomPositionPhysicalNode := vnode.nodeList[roomPosition]

				if msg.operation == "create" || msg.operation == "delete" || msg.operation == "read" {
					go vnode.requestReplicate(msg.msgRoomId, msg.msgStudentId, msg.operation)

				} else if msg.operation == "request" {
					// handle reqests for replies

					// physicalnodereply
					go vnode.nodeList[vnode.roomToPos[msg.msgRoomId]].physicalNodeReply(msg)

				} else if msg.operation == "reply" {

					// receive replies to add to replycount
					key := strconv.Itoa(msg.msgRoomId) + strconv.Itoa(msg.msgStudentId)

					vnode.pnodeMutex.Lock()
					// if vnode.pnodeReplies[key].localVectorClock[msg.msgRoomId] < msg.senderPnode.localVectorClock[msg.msgRoomId] {
					// 	vnode.pnodeReplies[key].localVectorClock[msg.msgRoomId] = msg.senderPnode.localVectorClock[msg.msgRoomId]
					// }

					if vnode.nodeList[vnode.roomToPos[msg.msgRoomId]].localLogicalClock < msg.senderPnode.localLogicalClock {
						vnode.nodeList[vnode.roomToPos[msg.msgRoomId]].localLogicalClock = msg.senderPnode.localLogicalClock
						vnode.nodeList[vnode.roomToPos[msg.msgRoomId]].studentID = msg.senderPnode.studentID
					}
					vnode.pnodeMutex.Unlock()

					vnode.vnodeMutex.Lock()
					vnode.replyCount[key] += 1
					vnode.vnodeMutex.Unlock()

				} else if msg.operation == "create-success" {
					if roomPositionPhysicalNode.studentID != 0 {
						// if room is booked - print already booked
						fmt.Println("The room has already been booked by Student ID: " + strconv.Itoa(roomPositionPhysicalNode.studentID))
					} else {
						// if room is NOT booked - allow booking
						// update create operation here locally
						roomPositionPhysicalNode.studentID = msg.msgStudentId

						// request other vnodes to replicate data
						updateMSG := new(message)
						updateMSG.msgRoomId = msg.msgRoomId
						updateMSG.msgStudentId = msg.msgStudentId
						updateMSG.operation = "update"
						updateMSG.sender = vnode
						updateMSG.senderPnode = roomPositionPhysicalNode // updated pnode

						for i := 1; i < numOfReplicas; i++ {
							replicaVnode := allVirtualNodes[(vnode.hashID+i)%numOfVirtualNodes]
							replicaVnode.ch <- *updateMSG
						}

						fmt.Println("The room " + strconv.Itoa(msg.msgRoomId) + " has been successfully booked by " + strconv.Itoa(msg.msgStudentId))
					}

					key := strconv.Itoa(msg.msgRoomId) + strconv.Itoa(msg.msgStudentId)
					vnode.vnodeMutex.Lock()
					vnode.replyCount[key] = 0
					vnode.vnodeMutex.Unlock()

				} else if msg.operation == "delete-success" {

					if roomPositionPhysicalNode.studentID == msg.msgStudentId {
						// if room is booked by CORRECT student - delete booking
						// update delete operation here locally
						roomPositionPhysicalNode.studentID = 0
						fmt.Println("Booking deleted")
					} else if roomPositionPhysicalNode.studentID != 0 {
						// if room is booked by WRONG student - CANNOT delete booking
						fmt.Println("Unable to delete someone else's booking")
					} else {
						// if room not booked - CANNOT delete booking
						fmt.Println("Room not booked - No booking to delete")
					}

					// request other vnodes to replicate data
					updateMSG := new(message)
					updateMSG.msgRoomId = msg.msgRoomId
					updateMSG.msgStudentId = msg.msgStudentId
					updateMSG.operation = "update"
					updateMSG.sender = vnode
					updateMSG.senderPnode = roomPositionPhysicalNode // updated pnode

					for i := 1; i < numOfReplicas; i++ {
						replicaVnode := allVirtualNodes[(vnode.hashID+i)%numOfVirtualNodes]
						replicaVnode.ch <- *updateMSG
					}

					key := strconv.Itoa(msg.msgRoomId) + strconv.Itoa(msg.msgStudentId)
					vnode.vnodeMutex.Lock()
					vnode.replyCount[key] = 0
					vnode.vnodeMutex.Unlock()
					fmt.Println("delete done")

				} else if msg.operation == "read-success" {
					if roomPositionPhysicalNode.studentID == 0 {
						fmt.Println("Room " + strconv.Itoa(roomPositionPhysicalNode.roomID) + " not booked")
					} else {
						fmt.Println("Room " + strconv.Itoa(roomPositionPhysicalNode.roomID) + " is booked by Student ID: " + strconv.Itoa(roomPositionPhysicalNode.studentID))
					}

				} else if msg.operation == "update" {
					fmt.Println("Node " + strconv.Itoa(vnode.hashID) + " updating replica of roomID " + strconv.Itoa(roomPositionPhysicalNode.roomID))
					roomPositionPhysicalNode.studentID = msg.senderPnode.studentID
					roomPositionPhysicalNode.localLogicalClock = msg.senderPnode.localLogicalClock
				} else if msg.operation == "reply from pnode" {
					for idx, replica := range vnode.replicaAliveArr {
						if msg.sender == replica {
							if idx == 1 {
								// remove the second element
								vnode.replicaAliveArr[0] = vnode.replicaAliveArr[1]
							} else if idx == 0 {
								// remove first element, and move second element forward
								vnode.replicaAliveArr[0] = vnode.replicaAliveArr[1]
								vnode.replicaAliveArr[1] = nil
							}
							break
						}
					}
				}
			}
		}
	}
}

// 1) pnode dies, vnode alive
// 2) vnode dies, pnode alive
// 3) both dies???

// func (pnode *physicalNode) updateReplica(updatedPnode *physicalNode) {
// 	pnode.studentID = updatedPnode.studentID
// 	pnode.localLogicalClock = updatedPnode.localLogicalClock
// }

func (pnode *physicalNode) physicalNodeReply(msg message) {
	fmt.Println("Physical Node " + strconv.Itoa(pnode.roomID) + " is replying to replication request")
	senderCH := msg.sender.ch

	newMSG := new(message)
	newMSG.operation = "reply"
	newMSG.msgRoomId = msg.msgRoomId
	newMSG.msgStudentId = msg.msgStudentId
	newMSG.senderPnode = pnode
	senderCH <- *newMSG

}

func (vnode *virtualNode) requestReplicate(roomID int, studentID int, op string) {
	// this is the go routine that will busy wait for replies
	fmt.Println("Requesting replication")

	newMSG := new(message)
	newMSG.operation = "request"
	newMSG.msgRoomId = roomID
	newMSG.msgStudentId = studentID
	newMSG.sender = vnode

	for i := 1; i < numOfReplicas; i++ {
		next := (vnode.hashID + i) % numOfVirtualNodes
		nextCH := allVirtualNodes[next].ch
		nextCH <- *newMSG
	}

	// initialise replyCount
	key := strconv.Itoa(roomID) + strconv.Itoa(studentID)
	vnode.vnodeMutex.Lock()
	vnode.replyCount[key] = 0
	vnode.vnodeMutex.Unlock()

	// initialise pnodeReplies
	vnode.pnodeMutex.Lock()
	vnode.pnodeReplies[key] = vnode.nodeList[vnode.roomToPos[roomID]]
	vnode.pnodeMutex.Unlock()

	// wait for replies
	start := time.Now()
	for {
		// if majority, break (SUCCESS)

		if op == "create" || op == "delete" {
			// for write operation
			vnode.vnodeMutex.Lock()
			if vnode.replyCount[key] >= wReplication-1 {
				vnode.vnodeMutex.Unlock()
				// send message back to virtualNodeWait() - successful replication
				successMSG := new(message)
				successMSG.operation = op + "-success"
				successMSG.msgRoomId = roomID
				successMSG.msgStudentId = studentID
				successMSG.sender = vnode
				vnode.ch <- *successMSG
				return
			}
			vnode.vnodeMutex.Unlock()
		} else if op == "read" {
			// for read operation
			vnode.vnodeMutex.Lock()
			if vnode.replyCount[key] >= rReplication {
				vnode.vnodeMutex.Unlock()
				// send message back to virtualNodeWait() - successful replication
				successMSG := new(message)
				successMSG.operation = op + "-success"
				successMSG.msgRoomId = roomID
				successMSG.msgStudentId = studentID
				successMSG.sender = vnode
				vnode.ch <- *successMSG
				return
			}
			vnode.vnodeMutex.Unlock()
		}

		// if timeout, replication failed (FAILURE)
		elapsed := time.Since(start)
		if elapsed >= time.Second*5 {
			// did not receive response from at least one replica
			fmt.Println("At least one replica has died..")
			// check if other two replicas are alive

			for i := 1; i < numOfReplicas; i++ {
				replicaVnode := allVirtualNodes[(vnode.hashID+i)%numOfVirtualNodes]
				replicaPnode := replicaVnode.nodeList[vnode.roomToPos[roomID]]
				replicaPnode.ch <- (vnode.hashID + i) % numOfVirtualNodes // check if replica is dead or alive
				vnode.replicaAliveArr = append(vnode.replicaAliveArr, replicaVnode)
			}
			start := time.Now()
			for {
				if len(vnode.replicaAliveArr) == 0 {
					fmt.Println("Replicas are alive")
					return
				}
				elapsed2 := time.Since(start)
				if elapsed2 >= time.Second*5 {
					// check if receive replies
					for _, replica := range vnode.replicaAliveArr {
						fmt.Println("Replica " + strconv.Itoa(replica.hashID) + " is dead")

						// transfer replica data to new physical node
						vnode.createNewReplicaData(replica.hashID, roomID)
					}
					return
				}
			}
		}
	}
}

func (vnode *virtualNode) createNewReplicaData(vnodeID int, roomID int) {
	// to be continued
	newPhysicalNode := physicalNode{roomID: , studentID: , localLogicalClock: , parentVnode: }
}

func (pnode *physicalNode) physicalNodeWait() {
	physicalNodeCh := pnode.ch
	for {

		// will take in input from general function which is the HASHED room id to be querried
		// will then identify the physical node that contains the HASHED room id
		select {
		case sender, ok := <-physicalNodeCh:
			if ok {
				newMSG := new(message)
				newMSG.sender = pnode.parentVnode
				newMSG.operation = "reply from pnode"
				allVirtualNodes[sender].ch <- *newMSG
			}
		}
	}
}

func (vnode *virtualNode) replicateData(roomID int, studentID int) {
	fmt.Println("Replication request successfull, replicating data")

	// get next replicase
	for i := 1; i < numOfReplicas; i++ {
		next := (vnode.hashID + i) % numOfVirtualNodes
		physicalNode := allVirtualNodes[next].nodeList[vnode.roomToPos[roomID]]
		physicalNode.studentID = studentID
	}
	fmt.Println("Replication successful!")
}

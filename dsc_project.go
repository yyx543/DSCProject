package main

import (
	"fmt"
	"strconv"
	"time"
)

var allVirtualNodes []*virtualNode // global list of virtual nodes
var numOfVirtualNodes int = 5      // assume 5 virtual nodes
var numOfRoomIds int = 10          // assume 10 different rooms
var numOfReplicas int = 3          // assume no replication
const rReplication int = 2
const wReplication int = 2

func main() {

	allVirtualNodes = make([]*virtualNode, numOfVirtualNodes)

	// room id (0 - 9)
	// roomId // numOfVirtualNodes -> [0, 4]

	// create virtual nodes
	for id := 0; id < numOfVirtualNodes; id++ {
		nodeNew := virtualNode{ch: make(chan message), nodeList: make([]*physicalNode, numOfRoomIds/numOfVirtualNodes*numOfReplicas), hashID: id, roomToPos: make(map[int]int)}
		for j := 0; j < numOfRoomIds/numOfVirtualNodes; j++ {
			for k := 0; k < numOfReplicas; k++ {
				var newPhysicalNode physicalNode
				if k == 0 {
					newPhysicalNode = physicalNode{roomID: (id + k + j*numOfVirtualNodes) % numOfRoomIds, studentID: 0}

					nodeNew.roomToPos[(id+k+j*numOfVirtualNodes)%numOfRoomIds] = j + k*numOfRoomIds/numOfVirtualNodes
				} else {
					newPhysicalNode = physicalNode{roomID: (id + k + (numOfVirtualNodes - numOfReplicas) + j*numOfVirtualNodes) % numOfRoomIds, studentID: 0}

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
	ch          chan message
	nodeList    []*physicalNode
	hashID      int
	roomToPos   map[int]int // mapping between physical node roomID (key) and its position in nodeList (value)
	replyCount  int
	requestDONE bool
	// requestBoolean bool
}

type message struct {
	msgRoomId    int
	msgStudentId int
	operation    string // "create", "delete"
	sender       *virtualNode
}
type physicalNode struct {
	roomID    int
	studentID int
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

				if msg.operation == "create" {
					if roomPositionPhysicalNode.studentID != 0 {
						// if room is booked - print already booked
						fmt.Println("The room has already been booked by Student ID: " + strconv.Itoa(roomPositionPhysicalNode.studentID))
					} else {
						// if room is NOT booked - allow booking
						vnode.requestReplicate(msg.msgRoomId, msg.msgStudentId)
						start := time.Now()
						for vnode.requestDONE == false {
							//fmt.Println(vnode.replyCount)
							// if vnode.requestBoolean {
							// 	physicalNode := vnode.nodeList[vnode.roomToPos[msg.msgRoomId]]
							// 	physicalNode.physicalNodeReply(msg)
							// 	vnode.requestBoolean = false
							// }
							if vnode.replyCount >= wReplication {
								vnode.replicateData(msg.msgRoomId, msg.msgStudentId)
								vnode.requestDONE = true
								//reset
								vnode.replyCount = 0
								break

							}
							elapsed := time.Since(start)
							if elapsed >= time.Second*5 {
								fmt.Println("ERROR REPLICATION HAS FAILED")
								return
							}
						}

						roomPositionPhysicalNode.studentID = msg.msgStudentId

						fmt.Println("The room " + strconv.Itoa(msg.msgRoomId) + " has been successfully booked by " + strconv.Itoa(msg.msgStudentId))
					}
				} else if msg.operation == "delete" {
					if roomPositionPhysicalNode.studentID == msg.msgStudentId {
						// if room is booked by CORRECT student - delete booking

						vnode.requestReplicate(msg.msgRoomId, msg.msgStudentId)
						start := time.Now()
						for vnode.requestDONE == false {
							if vnode.replyCount >= wReplication {
								vnode.replicateData(msg.msgRoomId, msg.msgStudentId)
								vnode.requestDONE = true
								//reset
								vnode.replyCount = 0
								break

							}
							elapsed := time.Since(start)
							if elapsed >= time.Second*5 {
								fmt.Println("ERROR REPLICATION HAS FAILED")
								return
							}
						}

						roomPositionPhysicalNode.studentID = 0
						fmt.Println("Booking deleted")
					} else if roomPositionPhysicalNode.studentID != 0 {
						// if room is booked by WRONG student - CANNOT delete booking
						fmt.Println("Unable to delete someone else's booking")
					} else {
						// if room not booked - CANNOT delete booking
						fmt.Println("Room not booked - No booking to delete")
					}
				} else if msg.operation == "request" {
					physicalNode := vnode.nodeList[vnode.roomToPos[msg.msgRoomId]]
					physicalNode.physicalNodeReply(msg)
				} else if msg.operation == "reply" {
					// vnode.replyCount++
					// fmt.Println("reply count " + strconv.Itoa(vnode.replyCount))
					// if vnode.replyCount >= wReplication {
					// 	vnode.replicateData(msg.msgRoomId, msg.msgStudentId)
					// 	vnode.requestDONE = true
					// 	//reset
					// 	vnode.replyCount = 0

					// }
				}
			}
		}
	}
}

func (pnode *physicalNode) physicalNodeReply(msg message) {
	fmt.Println("Physical Node " + strconv.Itoa(pnode.roomID) + " is replying to replication request")
	senderCH := msg.sender.ch
	msg.sender.replyCount += 1
	// fmt.Println(msg.sender.replyCount)

	newMSG := new(message)
	newMSG.operation = "reply"
	newMSG.msgRoomId = msg.msgRoomId
	newMSG.msgStudentId = msg.msgStudentId
	senderCH <- *newMSG

}

func (vnode *virtualNode) requestReplicate(roomID int, studentID int) {
	fmt.Println("Requesting replication")

	//get next 2 virtual nodes id
	next1 := (vnode.hashID + 1) % numOfVirtualNodes
	next2 := (vnode.hashID + 2) % numOfVirtualNodes

	//request msg
	next1CH := allVirtualNodes[next1].ch
	next2CH := allVirtualNodes[next2].ch

	newMSG := new(message)
	newMSG.operation = "request"
	newMSG.msgRoomId = roomID
	newMSG.msgStudentId = studentID
	newMSG.sender = vnode

	next1CH <- *newMSG
	next2CH <- *newMSG
	// vnode.requestBoolean = true
	// allVirtualNodes[next1].requestBoolean = true
	// allVirtualNodes[next2].requestBoolean = true

}

func (vnode *virtualNode) replicateData(roomID int, studentID int) {
	fmt.Println("Replication request successfull, replicating data")
	//get next 2
	next1 := (vnode.hashID + 1) % numOfVirtualNodes
	next2 := (vnode.hashID + 2) % numOfVirtualNodes

	physicalNode1 := allVirtualNodes[next1].nodeList[vnode.roomToPos[roomID]]
	physicalNode1.studentID = studentID

	physicalNode2 := allVirtualNodes[next2].nodeList[vnode.roomToPos[roomID]]
	physicalNode2.studentID = studentID

}

// func (c client) sendMessage(msg message) { //client struc method
// 	ch := c.server.ch
// 	c.vectorClock[c.id] += 1
// 	//vclock := c.vectorClock

// 	msg.vectorClock = c.vectorClock
// 	ch <- msg // this sends a message to server channel

// }

// func (s server) serverWAIT() {
// 	serverchannel := s.ch
// 	for {
// 		select {
// 		case i, ok := <-serverchannel:
// 			if ok {
// 				go s.serverBroadcast(i)

// 			}
// 		case <-exitChannel:
// 			fmt.Println("Server closing")
// 			return
// 		default:

// 		}
// 	}
// }

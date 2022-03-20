package main

import (
	"fmt"
	"strconv"
	"time"
)

var allVirtualNodes []*virtualNode // global list of virtual nodes
var numOfVirtualNodes int = 5      // assume 5 virtual nodes
var numOfRoomIds int = 10          // assume 10 different rooms
var numOfReplicas int = 1          // assume no replication

func main() {

	allVirtualNodes = make([]*virtualNode, numOfVirtualNodes)

	// room id (0 - 9)
	// roomId // numOfVirtualNodes -> [0, 4]

	// create virtual nodes
	for id := 0; id < numOfVirtualNodes; id++ {
		nodeNew := virtualNode{ch: make(chan message), nodeList: make([]*physicalNode, numOfRoomIds/numOfVirtualNodes*numOfReplicas), hashID: id, roomToPos: make(map[int]int)}
		// nodeNEW := new(virtualNode)
		// nodeNEW.nodeList := make([]*physicalNode, numOfRoomIds/numOfVirtualNodes*numOfReplicas)

		// create list of physical nodes
		for j := 0; j < numOfRoomIds/numOfVirtualNodes; j++ {
			newPhysicalNode := physicalNode{roomID: id + j*numOfVirtualNodes, studentID: 0}
			nodeNew.nodeList[j] = &newPhysicalNode
			nodeNew.roomToPos[id+j*numOfVirtualNodes] = j
		}

		go nodeNew.virtualNodeWait()
		allVirtualNodes[id] = &nodeNew
	}
	fmt.Println("Length of allvirtualnodes " + strconv.Itoa(len(allVirtualNodes)))
	go generalWait(1, "delete", 1004123)
	time.Sleep(3 * time.Second)
	//go generalWait(1, "delete", 1004123)

	for {

	}
}

func generalWait(roomId int, op string, studentId int) { // op can be "create" or "delete"
	// hash room id
	inputHashId := roomId % numOfVirtualNodes
	fmt.Println("Entering General wait")
	fmt.Println(op + " room " + strconv.Itoa(roomId) + " by studentID: " + strconv.Itoa(studentId))

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
}

type message struct {
	msgRoomId    int
	msgStudentId int
	operation    string // "create", "delete"
}

func (vnode virtualNode) virtualNodeWait() {
	fmt.Println("Node " + strconv.Itoa(vnode.hashID) + " is now waiting")
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
						roomPositionPhysicalNode.studentID = msg.msgStudentId
						fmt.Println("The room " + strconv.Itoa(msg.msgRoomId) + " has been successfully booked by " + strconv.Itoa(msg.msgStudentId))
					}
				} else if msg.operation == "delete" {
					if roomPositionPhysicalNode.studentID == msg.msgStudentId {
						// if room is booked by CORRECT student - delete booking
						roomPositionPhysicalNode.studentID = 0
						fmt.Println("Booking deleted")
					} else if roomPositionPhysicalNode.studentID != 0 {
						// if room is booked by WRONG student - CANNOT delete booking
						fmt.Println("Unable to delete someone else's booking")
					} else {
						// if room not booked - CANNOT delete booking
						fmt.Println("Room not booked - No booking to delete")
					}
				}
			}
		}
	}
}

type physicalNode struct {
	roomID    int
	studentID int
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

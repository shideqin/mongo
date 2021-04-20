package main

import (
	"fmt"

	"github.com/shideqin/mongo"
)

func main() {
	//接连
	client := mongo.Conn("127.0.0.1:27017")
	fmt.Println(client)

	//监测连接错误
	err := mongo.Ping()
	fmt.Println(err, mongo.ErrNotFound)

	//获取objectID
	objectID := mongo.NewObjectID()
	fmt.Println(objectID)

	//object转成16进制
	hexID := mongo.Hex(objectID)
	fmt.Println(hexID)

	//是否为ObjectId的有效十六进制
	isObject := mongo.IsObjectIdHex(hexID)
	fmt.Println(isObject)

	//16进制转成objectID
	newObjectID := mongo.ObjectIDHex(hexID)
	fmt.Println(newObjectID)
}

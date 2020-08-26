package mongo

import (
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

var (
	//ErrNotFound 数据没有找到
	ErrNotFound = mgo.ErrNotFound
	//connErr 连接错误
	connErr error
)

//M 自定义bson类型
type M bson.M

//Sort 自定义排序类型
type Sort []string

//ObjectID 自定义ObjectID类型
type ObjectID interface{}

//Client mongodb连接结构体
type Client struct {
	session *mgo.Session
}

//Conn 连接mongodb
func Conn(url string) *Client {
	defer func() {
		if r := recover(); r != nil {
			connErr = r.(error)
		}
	}()
	//[mongodb://][user:pass@]host1[:port1][,host2[:port2],...][/database][?options]
	session, err := mgo.Dial(url)
	if err != nil {
		panic(err)
	}
	connErr = nil
	session.SetSocketTimeout(24 * time.Hour)

	// Optional. Switch the session to a monotonic behavior.
	//session.SetMode(mgo.Monotonic, true)
	return &Client{
		session: session,
	}
}

//Ping 监测数据库连接
func Ping() error {
	return connErr
}

//NewObjectID 返回一个新的唯一ObjectId
func NewObjectID() ObjectID {
	return bson.NewObjectId()
}

//Hex 返回ObjectId的十六进制表示
func Hex(id ObjectID) string {
	return id.(bson.ObjectId).Hex()
}

//ObjectIDHex 将id转成十六进制表示返回ObjectId
func ObjectIDHex(id string) ObjectID {
	return bson.ObjectIdHex(id)
}

//GetRow 返回一行数据
func (c *Client) GetRow(database, collection string, query M, result interface{}) error {
	if connErr != nil {
		return connErr
	}
	session := c.session.Copy()
	defer session.Close()
	conn := session.DB(database).C(collection)
	//query MongoDB
	return conn.Find(query).One(result)
}

//GetResult 返回多行结果集
func (c *Client) GetResult(database, collection string, query M, fields M, options M, result interface{}) error {
	if connErr != nil {
		return connErr
	}
	session := c.session.Copy()
	defer session.Close()
	conn := session.DB(database).C(collection)
	find := conn.Find(query).Select(fields)
	//排序
	if options["Sort"] != "" {
		if sort, ok := options["Sort"].(Sort); ok {
			find.Sort(sort...)
		}
	}
	//分页
	if options["Limit"] != "" {
		if limit, ok := options["Limit"].(int); ok {
			find.Limit(limit)
		}
	}
	//跳过
	if options["Skip"] != "" {
		if skip, ok := options["Skip"].(int); ok {
			find.Skip(skip)
		}
	}
	return find.All(result)
}

//GetCount 返回统计条数
func (c *Client) GetCount(database, collection string, query M) (int, error) {
	if connErr != nil {
		return 0, connErr
	}
	session := c.session.Copy()
	defer session.Close()
	conn := session.DB(database).C(collection)
	//query MongoDB
	return conn.Find(query).Count()
}

//Insert 插入数据
func (c *Client) Insert(database, collection string, docs ...interface{}) error {
	if connErr != nil {
		return connErr
	}
	session := c.session.Copy()
	defer session.Close()
	conn := session.DB(database).C(collection)
	return conn.Insert(docs...)
}

//Update 更新数据,不存在报ErrNotFound
func (c *Client) Update(database, collection string, selector M, update M) error {
	if connErr != nil {
		return connErr
	}
	session := c.session.Copy()
	defer session.Close()
	conn := session.DB(database).C(collection)
	return conn.Update(selector, update)
}

//UpdateAll 批量更新数据,不存在报ErrNotFound
func (c *Client) UpdateAll(database, collection string, selector M, update M) (map[string]interface{}, error) {
	if connErr != nil {
		return map[string]interface{}{}, connErr
	}
	session := c.session.Copy()
	defer session.Close()
	conn := session.DB(database).C(collection)
	info, err := conn.UpdateAll(selector, update)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"Matched": info.Matched, "Updated": info.Updated, "UpsertedId": info.UpsertedId}, nil
}

//Upsert 更新数据,不存在会新插入数据
func (c *Client) Upsert(database, collection string, selector M, update M) (map[string]interface{}, error) {
	if connErr != nil {
		return map[string]interface{}{}, connErr
	}
	session := c.session.Copy()
	defer session.Close()
	conn := session.DB(database).C(collection)
	info, err := conn.Upsert(selector, update)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"Matched": info.Matched, "Updated": info.Updated, "UpsertedId": info.UpsertedId}, nil
}

//Remove 删除数据
func (c *Client) Remove(database, collection string, selector M) error {
	if connErr != nil {
		return connErr
	}
	session := c.session.Copy()
	defer session.Close()
	conn := session.DB(database).C(collection)
	return conn.Remove(selector)
}

//RemoveAll 批量删除数据
func (c *Client) RemoveAll(database, collection string, selector M) (int, error) {
	if connErr != nil {
		return 0, connErr
	}
	session := c.session.Copy()
	defer session.Close()
	conn := session.DB(database).C(collection)
	info, err := conn.RemoveAll(selector)
	var removed int
	if err == nil {
		removed = info.Removed
	}
	return removed, err
}

//FindAndModify 查找并修改数据
func (c *Client) FindAndModify(database, collection string, selector M, update M, upsert bool, result interface{}) (int, error) {
	if connErr != nil {
		return 0, connErr
	}
	session := c.session.Copy()
	defer func() {
		if r := recover(); r != nil {
		}
		session.Close()
	}()
	change := mgo.Change{Update: update, Upsert: upsert, ReturnNew: true}
	conn := session.DB(database).C(collection)
	info, err := conn.Find(selector).Apply(change, result)
	var updated int
	if err == nil {
		updated = info.Updated
	}
	return updated, err
}

//FindAndRemove 查找并删除数据
func (c *Client) FindAndRemove(database, collection string, selector M, result interface{}) (int, error) {
	if connErr != nil {
		return 0, connErr
	}
	session := c.session.Copy()
	defer func() {
		if r := recover(); r != nil {
		}
		session.Close()
	}()
	change := mgo.Change{Remove: true}
	conn := session.DB(database).C(collection)
	info, err := conn.Find(selector).Apply(change, result)
	var removed int
	if err == nil {
		removed = info.Removed
	}
	return removed, err
}

//GetPipeRow 使用管道进行聚合计算并返回一行数据
func (c *Client) GetPipeRow(database, collection string, pipeline []M, result *M) error {
	if connErr != nil {
		return connErr
	}
	session := c.session.Copy()
	defer func() {
		if r := recover(); r != nil {
		}
		session.Close()
	}()
	conn := session.DB(database).C(collection)
	return conn.Pipe(pipeline).One(result)
}

//GetPipeResult 使用管道进行聚合计算并返回多行结果集
func (c *Client) GetPipeResult(database, collection string, pipeline []M, result *[]M) error {
	if connErr != nil {
		return connErr
	}
	session := c.session.Copy()
	defer func() {
		if r := recover(); r != nil {
		}
		session.Close()
	}()
	conn := session.DB(database).C(collection)
	return conn.Pipe(pipeline).All(result)
}

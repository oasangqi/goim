package logic

import (
	"context"

	"github.com/Terry-Mao/goim/internal/logic/model"

	log "github.com/golang/glog"
)

// PushKeys push a message by keys.
func (l *Logic) PushKeys(c context.Context, op int32, keys []string, msg []byte) (err error) {
	// servers和keys按顺序对应
	servers, err := l.dao.ServersByKeys(c, keys)
	if err != nil {
		return
	}
	// map serverID:keys
	pushKeys := make(map[string][]string)
	for i, key := range keys {
		server := servers[i]
		if server != "" && key != "" {
			pushKeys[server] = append(pushKeys[server], key)
		}
	}
	// 推送给serverID上的keys(生产kafka消息)
	for server := range pushKeys {
		if err = l.dao.PushMsg(c, op, server, pushKeys[server], msg); err != nil {
			return
		}
	}
	return
}

// PushMids push a message by mid.
func (l *Logic) PushMids(c context.Context, op int32, mids []int64, msg []byte) (err error) {
	// 从redis获取map key:serverID
	keyServers, _, err := l.dao.KeysByMids(c, mids)
	if err != nil {
		return
	}
	// map serverID:keys
	keys := make(map[string][]string)
	for key, server := range keyServers {
		if key == "" || server == "" {
			log.Warningf("push key:%s server:%s is empty", key, server)
			continue
		}
		keys[server] = append(keys[server], key)
	}
	// 推送给serverID上的keys(生产kafka消息)
	for server, keys := range keys {
		if err = l.dao.PushMsg(c, op, server, keys, msg); err != nil {
			return
		}
	}
	return
}

// PushRoom push a message by room.
func (l *Logic) PushRoom(c context.Context, op int32, typ, room string, msg []byte) (err error) {
	return l.dao.BroadcastRoomMsg(c, op, model.EncodeRoomKey(typ, room), msg)
}

// PushAll push a message to all.
func (l *Logic) PushAll(c context.Context, op, speed int32, msg []byte) (err error) {
	return l.dao.BroadcastMsg(c, op, speed, msg)
}

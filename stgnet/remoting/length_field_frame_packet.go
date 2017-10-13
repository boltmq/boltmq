package remoting

import (
	"bytes"
	"encoding/binary"
	"sync"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"github.com/go-errors/errors"
)

type LengthFieldFramePacket struct {
	maxFrameLength      int                      // 最大帧的长度
	lengthFieldOffset   int                      // 长度属性的起始偏移量
	lengthFieldLength   int                      // 长度属性的长度
	initialBytesToStrip int                      // 业务数据需要跳过的长度
	bufTableLock        sync.RWMutex             // 报文缓存的读写锁
	bufTable            map[string]*bytes.Buffer // 按连接地址对包进行处理，每个连接有独立goroutine处理。
}

func NewLengthFieldFramePacket(maxFrameLength, lengthFieldOffset, lengthFieldLength, initialBytesToStrip int) *LengthFieldFramePacket {
	return &LengthFieldFramePacket{
		maxFrameLength:      maxFrameLength,
		lengthFieldOffset:   lengthFieldOffset,
		lengthFieldLength:   lengthFieldLength,
		initialBytesToStrip: initialBytesToStrip,
		bufTable:            make(map[string]*bytes.Buffer),
	}
}

func (lffp *LengthFieldFramePacket) UnPack(addr string, buffer []byte) (bufs []*bytes.Buffer, e error) {
	var (
		length = len(buffer)
	)

	if length > lffp.maxFrameLength {
		// 报文长度大于设置最大长度，丢弃报文（之后考虑其它方式）
		logger.Errorf("buffer length[%d] > maxFrameLength[%d], discard.", length, lffp.maxFrameLength)
		e = errors.Errorf("buffer length[%d] > maxFrameLength[%d], discard.", length, lffp.maxFrameLength)
		return
	}

	// 缓存报文
	lffp.bufTableLock.RLock()
	buf, ok := lffp.bufTable[addr]
	lffp.bufTableLock.RUnlock()
	if !ok {
		buf = &bytes.Buffer{}
		lffp.bufTableLock.Lock()
		lffp.bufTable[addr] = buf
		lffp.bufTableLock.Unlock()
	}

	_, e = buf.Write(buffer)
	if e != nil {
		e = errors.Wrap(e, 0)
		return
	}

	return lffp.readBuffer(addr, buf)
}

func (lffp *LengthFieldFramePacket) readBuffer(addr string, buf *bytes.Buffer) (bufs []*bytes.Buffer, e error) {
	var (
		start       int
		end         int
		length      int
		frameLength int
	)

	start = lffp.lengthFieldOffset
	end = lffp.lengthFieldOffset + lffp.lengthFieldLength

	for {
		length = buf.Len()
		if length <= end {
			// 长度不够，等待下个报文。
			break
		}

		// 读取报文长度
		lengthFieldBytes := buf.Bytes()[start:end]
		frameLength, e = lffp.readLengthFieldLength(lengthFieldBytes)
		if e != nil {
			break
		}

		// 报文传输出错或报文到达顺序与发送顺序不一致，顺序问题之后考虑。
		if frameLength > lffp.maxFrameLength {
			// 丢弃报文
			lffp.bufTableLock.Lock()
			delete(lffp.bufTable, addr)
			lffp.bufTableLock.Unlock()
			logger.Errorf("frame length[%d] > maxFrameLength[%d], discard.", frameLength, lffp.maxFrameLength)
			e = errors.Errorf("frame length[%d] > maxFrameLength[%d], discard.", frameLength, lffp.maxFrameLength)
			break
		}

		// 长度小于报文长度，等待下个报文
		if length-end < frameLength {
			break
		}

		// 报文长度足够，读取报文并调整buffer
		rbuf, e := lffp.adjustBuffer(addr, buf, frameLength+end)
		if e != nil {
			break
		}
		bufs = append(bufs, rbuf)
	}

	return
}

func (lffp *LengthFieldFramePacket) adjustBuffer(addr string, buf *bytes.Buffer, frameLength int) (*bytes.Buffer, error) {
	// buffer中报文长度
	distance := buf.Len() - frameLength
	if distance == 0 {
		// buffer数据已经读取完
		lffp.bufTableLock.Lock()
		delete(lffp.bufTable, addr)
		lffp.bufTableLock.Unlock()
	}

	// 读取报文掉过的长度
	if lffp.initialBytesToStrip > 0 {
		buf.Next(lffp.initialBytesToStrip)
	}

	// 读取报文
	buffer := buf.Next(frameLength)

	nbuf := &bytes.Buffer{}
	_, err := nbuf.Write(buffer)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	return nbuf, nil
}

func (lffp *LengthFieldFramePacket) readLengthFieldLength(lengthFieldBytes []byte) (int, error) {
	var (
		frameLength int
	)
	switch lffp.lengthFieldLength {
	case 1:
		frameLength = int(lengthFieldBytes[0])
	case 2:
		lengthField := binary.BigEndian.Uint16(lengthFieldBytes)
		frameLength = int(lengthField)
	case 4:
		lengthField := binary.BigEndian.Uint32(lengthFieldBytes)
		frameLength = int(lengthField)
	case 8:
		lengthField := binary.BigEndian.Uint64(lengthFieldBytes)
		frameLength = int(lengthField)
	default:
		logger.Warnf("not support lengthFieldLength[%d].", lffp.lengthFieldLength)
		return 0, errors.Errorf("not support lengthFieldLength[%d].", lffp.lengthFieldLength)
	}

	return frameLength, nil
}

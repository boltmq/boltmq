package remoting

import (
	"bytes"
	"encoding/binary"
	"sync"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"github.com/go-errors/errors"
)

type LengthFieldFragmentationAssemblage struct {
	maxFrameLength       int                      // 最大帧的长度
	lengthFieldOffset    int                      // 长度属性的起始偏移量
	lengthFieldLength    int                      // 长度属性的长度
	initialBytesToStrip  int                      // 业务数据需要跳过的长度
	fragmentationTableMu sync.RWMutex             // 报文缓存的读写锁
	fragmentationTable   map[string]*bytes.Buffer // 按连接地址对包进行处理，每个连接有独立goroutine处理。
}

func NewLengthFieldFragmentationAssemblage(maxFrameLength, lengthFieldOffset, lengthFieldLength, initialBytesToStrip int) *LengthFieldFragmentationAssemblage {
	return &LengthFieldFragmentationAssemblage{
		maxFrameLength:      maxFrameLength,
		lengthFieldOffset:   lengthFieldOffset,
		lengthFieldLength:   lengthFieldLength,
		initialBytesToStrip: initialBytesToStrip,
		fragmentationTable:  make(map[string]*bytes.Buffer),
	}
}

func (lfpfa *LengthFieldFragmentationAssemblage) Pack(addr string, buffer []byte) (bufs []*bytes.Buffer, e error) {
	var (
		length = len(buffer)
	)

	if length > lfpfa.maxFrameLength {
		// 报文长度大于设置最大长度，丢弃报文（之后考虑其它方式）
		logger.Errorf("buffer length[%d] > maxFrameLength[%d], discard.", length, lfpfa.maxFrameLength)
		e = errors.Errorf("buffer length[%d] > maxFrameLength[%d], discard.", length, lfpfa.maxFrameLength)
		return
	}

	// 缓存报文
	lfpfa.fragmentationTableMu.RLock()
	buf, ok := lfpfa.fragmentationTable[addr]
	lfpfa.fragmentationTableMu.RUnlock()
	if !ok {
		buf = &bytes.Buffer{}
		lfpfa.fragmentationTableMu.Lock()
		lfpfa.fragmentationTable[addr] = buf
		lfpfa.fragmentationTableMu.Unlock()
	}

	_, e = buf.Write(buffer)
	if e != nil {
		e = errors.Wrap(e, 0)
		return
	}

	return lfpfa.pack(addr, buf)
}

func (lfpfa *LengthFieldFragmentationAssemblage) pack(addr string, buf *bytes.Buffer) (bufs []*bytes.Buffer, e error) {
	var (
		start        int
		end          int
		length       int
		packetLength int
	)

	start = lfpfa.lengthFieldOffset
	end = lfpfa.lengthFieldOffset + lfpfa.lengthFieldLength

	for {
		length = buf.Len()
		if length <= end {
			// 长度不够，等待下个报文。
			break
		}

		// 读取报文长度
		lengthFieldBytes := buf.Bytes()[start:end]
		packetLength, e = lfpfa.readLengthFieldLength(lengthFieldBytes)
		if e != nil {
			break
		}

		// 报文传输出错或报文到达顺序与发送顺序不一致，顺序问题之后考虑。
		if packetLength > lfpfa.maxFrameLength {
			// 丢弃报文
			lfpfa.fragmentationTableMu.Lock()
			delete(lfpfa.fragmentationTable, addr)
			lfpfa.fragmentationTableMu.Unlock()
			logger.Errorf("frame length[%d] > maxFrameLength[%d], discard.", packetLength, lfpfa.maxFrameLength)
			e = errors.Errorf("frame length[%d] > maxFrameLength[%d], discard.", packetLength, lfpfa.maxFrameLength)
			break
		}

		// 长度小于报文长度，等待下个报文
		if length-end < packetLength {
			break
		}

		// 报文长度足够，读取报文并调整buffer
		rbuf, e := lfpfa.adjustBuffer(addr, buf, packetLength+end)
		if e != nil {
			break
		}
		bufs = append(bufs, rbuf)
	}

	return
}

func (lfpfa *LengthFieldFragmentationAssemblage) adjustBuffer(addr string, buf *bytes.Buffer, packetLength int) (*bytes.Buffer, error) {
	// buffer中报文长度
	distance := buf.Len() - packetLength
	if distance == 0 {
		// buffer数据已经读取完
		lfpfa.fragmentationTableMu.Lock()
		delete(lfpfa.fragmentationTable, addr)
		lfpfa.fragmentationTableMu.Unlock()
	}

	// 读取报文掉过的长度
	if lfpfa.initialBytesToStrip > 0 {
		buf.Next(lfpfa.initialBytesToStrip)
		packetLength -= lfpfa.initialBytesToStrip
	}

	// 读取报文
	buffer := buf.Next(packetLength)

	nbuf := &bytes.Buffer{}
	_, err := nbuf.Write(buffer)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	return nbuf, nil
}

func (lfpfa *LengthFieldFragmentationAssemblage) readLengthFieldLength(lengthFieldBytes []byte) (int, error) {
	var (
		packetLength int
	)
	switch lfpfa.lengthFieldLength {
	case 1:
		packetLength = int(lengthFieldBytes[0])
	case 2:
		lengthField := binary.BigEndian.Uint16(lengthFieldBytes)
		packetLength = int(lengthField)
	case 4:
		lengthField := binary.BigEndian.Uint32(lengthFieldBytes)
		packetLength = int(lengthField)
	case 8:
		lengthField := binary.BigEndian.Uint64(lengthFieldBytes)
		packetLength = int(lengthField)
	default:
		logger.Warnf("not support lengthFieldLength[%d].", lfpfa.lengthFieldLength)
		return 0, errors.Errorf("not support lengthFieldLength[%d].", lfpfa.lengthFieldLength)
	}

	return packetLength, nil
}

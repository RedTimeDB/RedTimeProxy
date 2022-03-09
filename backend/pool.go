package backend

// 参考: https://blog.csdn.net/sh_frankie/article/details/75126463
type Pool struct {
	data chan []byte
	w    int
	h    int
}

func NewPool(maxSize int, w int) *Pool {
	return &Pool{
		data: make(chan []byte, maxSize),
		w:    w,
	}
}

// 获取 缓冲 池数据
func (p *Pool) Get() (b []byte) {
	select {
	case b = <-p.data:
	default:
		b = make([]byte, p.w)
	}
	return
}

// Get returns a byte slice with at least sz capacity.
func (p *Pool) GetBySize(sz int) (b []byte) {
	var c []byte
	select {
	case c = <-p.data:
	default:
		return make([]byte, sz)
	}

	if cap(c) < sz {
		return make([]byte, sz)
	}

	return c[:sz]
}

// Put returns a slice back to the pool
func (p *Pool) Put(c []byte) {
	if cap(c) < p.w {
		return
	}
	select {
	case p.data <- c[:p.w]:
	default:
	}
}

// GetPoolSize 获取到当前的 数据大小
func (p *Pool) GetPoolSize() int {
	return len(p.data)
}

// GetPoolWidth 获取池子宽
func (p *Pool) GetPoolWidth() int {
	return p.w
}

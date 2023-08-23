package single

import "io"

type callbackWriter struct {
	cb        func(n int)
	srcWriter io.Writer
}

func (cw *callbackWriter) Write(data []byte) (int, error) {
	n, err := cw.srcWriter.Write(data)
	cw.cb(n)
	return n, err
}

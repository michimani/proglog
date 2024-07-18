package log

var (
	Exported_lenWidth = lenWidth
	Exported_newStore = newStore
	Exported_enc      = enc
	Exported_entWidth = entWidth

	Exported_newIndex = newIndex

	Exported_newSegment = newSegment
)

type Exported_store = store

func (s *segment) Exported_NextOffset() uint64 {
	return s.nextOffset
}

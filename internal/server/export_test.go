package server

// Exported_records is a method used only during testing to directly access the Log.records.
// It returns a slice of records.
func (l *Log) Exported_records() []Record {
	if l == nil {
		return nil
	}

	return l.records
}

// Exported_setRecords is a method used only during testing to directly set the Log.records.
// It takes a slice of records as an argument and sets that slice to the Log.records.
func (l *Log) Exported_setRecords(records []Record) {
	if l == nil {
		return
	}

	l.records = records
}

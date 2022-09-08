package oracle

import (
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
)

func Raw(db *gorm.DB) {
	transferValType(db)
	callbacks.RawExec(db)
}

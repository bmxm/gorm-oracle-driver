package oracle

import (
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
)

func Query(db *gorm.DB) {
	if db.Error != nil {
		return
	}

	callbacks.BuildQuerySQL(db)
	transferValType(db)
	if !db.DryRun && db.Error == nil {
		rows, err := db.Statement.ConnPool.QueryContext(db.Statement.Context, db.Statement.SQL.String(), db.Statement.Vars...)
		if err != nil {
			db.AddError(err)
			return
		}
		defer func() {
			db.AddError(rows.Close())
		}()
		gorm.Scan(rows, db, 0)
	}
}

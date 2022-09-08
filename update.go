package oracle

import (
	"reflect"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/utils"
)

func BeforeUpdate(db *gorm.DB) {
	callbacks.BeforeUpdate(db)

	if db.Statement == nil {
		return
	}
	if destMap, ok := db.Statement.Dest.(map[string]interface{}); ok {
		newDestMap := make(map[string]interface{}, len(destMap))
		for key, val := range destMap {
			switch val.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string:

			default:
				// 自定义类型在 godror 会报 unknown type (如：stmt.go 1242)
				switch reflect.TypeOf(val).Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					val = reflect.ValueOf(val).Int()
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					val = reflect.ValueOf(val).Uint()
				case reflect.Float32, reflect.Float64:
					val = reflect.ValueOf(val).Float()
				case reflect.String:
					val = reflect.ValueOf(val).String()
				}
			}
			key = strings.Trim(key, `"`)
			newDestMap[`"`+strings.ToUpper(key)+`"`] = val
		}
		db.Statement.Dest = newDestMap
	}
}

func Update(config *callbacks.Config) func(db *gorm.DB) {
	supportReturning := utils.Contains(config.UpdateClauses, "RETURNING")

	return func(db *gorm.DB) {
		if db.Error != nil {
			return
		}

		if db.Statement.Schema != nil {
			for _, c := range db.Statement.Schema.UpdateClauses {
				db.Statement.AddClause(c)
			}
		}

		if db.Statement.SQL.Len() == 0 {
			db.Statement.SQL.Grow(180)
			db.Statement.AddClauseIfNotExists(clause.Update{})
			if set := callbacks.ConvertToAssignments(db.Statement); len(set) != 0 {
				db.Statement.AddClause(set)
			} else if _, ok := db.Statement.Clauses["SET"]; !ok {
				return
			}

			db.Statement.Build(db.Statement.BuildClauses...)
			transferValType(db)
		}

		checkMissingWhereConditions(db)

		if !db.DryRun && db.Error == nil {
			if ok, mode := hasReturning(db, supportReturning); ok {
				if rows, err := db.Statement.ConnPool.QueryContext(db.Statement.Context, db.Statement.SQL.String(), db.Statement.Vars...); db.AddError(err) == nil {
					dest := db.Statement.Dest
					db.Statement.Dest = db.Statement.ReflectValue.Addr().Interface()
					gorm.Scan(rows, db, mode)
					db.Statement.Dest = dest
					db.AddError(rows.Close())
				}
			} else {
				result, err := db.Statement.ConnPool.ExecContext(db.Statement.Context, db.Statement.SQL.String(), db.Statement.Vars...)

				if db.AddError(err) == nil {
					db.RowsAffected, _ = result.RowsAffected()
				}
			}
		}
	}
}

func checkMissingWhereConditions(db *gorm.DB) {
	if !db.AllowGlobalUpdate && db.Error == nil {
		where, withCondition := db.Statement.Clauses["WHERE"]
		if withCondition {
			if _, withSoftDelete := db.Statement.Clauses["soft_delete_enabled"]; withSoftDelete {
				whereClause, _ := where.Expression.(clause.Where)
				withCondition = len(whereClause.Exprs) > 1
			}
		}
		if !withCondition {
			db.AddError(gorm.ErrMissingWhereClause)
		}
		return
	}
}

func hasReturning(tx *gorm.DB, supportReturning bool) (bool, gorm.ScanMode) {
	if supportReturning {
		if c, ok := tx.Statement.Clauses["RETURNING"]; ok {
			returning, _ := c.Expression.(clause.Returning)
			if len(returning.Columns) == 0 || (len(returning.Columns) == 1 && returning.Columns[0].Name == "*") {
				return true, 0
			}
			return true, gorm.ScanUpdate
		}
	}
	return false, 0
}

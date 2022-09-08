

## Description

GORM Oracle driver for connect Oracle DB and Manage Oracle DB, Based on [CengSin/oracle](https://github.com/CengSin/oracle)
Oracle 表名和字段名必须大写。

## Required dependency Install

- Oracle 11g+
- Golang 1.13+
- see [ODPI-C Installation.](https://oracle.github.io/odpi/doc/installation.html)

## Quick Start
### how to install 
```bash
go get github.com/bmxm/gorm-oracle-driver
```
###  usage

```go
import (
    "gorm.io/gorm"

    "github.com/bmxm/gorm-oracle-driver"
)

func main() {
    db, err := gorm.Open(oracle.Open("oracle://username:password@127.0.0.1/servicename"), &gorm.Config{})
    if err != nil {
        // panic error or log error info
    } 
    
    // do somethings
}
```

package routine

import "errors"

/**
* golang-sample源代码，版权归锦翰科技（深圳）有限公司所有。
* <p>
* 文件名称 : errors.go
* 文件路径 :
* 作者 : DavidLiu
× Email: david.liu@ginghan.com
*
* 创建日期 : 2022/5/31 21:14
* 修改历史 : 1. [2022/5/31 21:14] 创建文件 by LongYong
*/

var (

	// ErrInvalidMaxIdle will be returned when setting a negative number as the periodic duration to purge goroutines.
	ErrInvalidMaxIdle = errors.New("invalid max idle for pool")
)

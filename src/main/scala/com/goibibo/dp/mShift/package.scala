package com.goibibo.dp

import java.util.ArrayList
package object mShift {
	//sourceFormat is for timestamp and date types of Redshift
	case class ColumnMapping(columnName: String, columnType: String, columnSource: String, sourceFormat: String)
    case class DataMapping(mongoUrl:String, redshiftUrl:String, 
    	tempS3Location:String, 
    	mongoFilterQuery:String,
    	redshiftTable:String, columns:Array[ColumnMapping], 
    	distStyle:String, distKey:String, sortKeySpec:String,
    	extraCopyOptions:String, preActions:String, postActions:String)
}





case "ADD" => Properties.addFile(messageText.substring(0, messageText.indexOf(":")), messageText.substring(messageText.indexOf(":") + 1))
case "UPDATE" => Properties.updateFile(messageText.substring(0, messageText.indexOf("#")).toInt, messageText.substring(messageText.indexOf("#") + 1, messageText.indexOf(":")), messageText.substring(messageText.indexOf(":") + 1))
case "REMOVE" => Properties.removeFile(messageText.toInt)
case "REFRESH" => Properties.refreshFile(messageText.toInt)

//CONNECTION - UPSERT - connection_type.connection_name=connection_string#&#user_name#&#password
//CONNECTION - ENABLE/DISABLE/REMOVE - connection_name
case "UPSERT" => JDBCConnection.upsert(messageText)
case "ENABLE" => JDBCConnection.enable(messageText)
case "DISABLE" => JDBCConnection.disable(messageText)
case "REMOVE" => JDBCConnection.remove(messageText)
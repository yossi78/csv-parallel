

#) How to run the "csv-standard" service 

#) Make sure your PC has been installed with JDK,Maven,IntelliJ and MIcrosoft Excel 

#) Open the project using IntelliJ aplication

# Run the service by running the main class called  "CsvStandardApplication"

#) Open api client such as "postman" and run followng API call: 
curl --location --request POST 'http://127.0.0.1:8080/csv/v1/sortByColumn' \
--header 'Content-Type: application/json' \
--data-raw '{
"filePath": "src\\main\\resources\\SOURCE__50000_ROWS__CSV.csv",
"compareIndex": 1,
"maxLineRead": 10   
}'




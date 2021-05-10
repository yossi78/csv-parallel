package com.veeva.csvparallel.api;
import com.veeva.csvparallel.dao.Product;
import com.veeva.csvparallel.dto.CsvSortColumnRequest;
import com.veeva.csvparallel.files.FileUtil;
import com.veeva.csvparallel.services.CsvService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;


@RestController
@RequestMapping(value = "/v1")
@Slf4j
public class CsvController {


    private CsvService csvService;


    @Autowired
    public CsvController(CsvService csvService) {
        this.csvService = csvService;
    }


    /**
     *  Run followng API call:  POST -> http://127.0.0.1:8080/csv/v1/sortByColumn
     with the request body of CsvSortColumnRequest
     */

    @PostMapping(value = {"/sortByColumn"})
    public ResponseEntity sortCsvFile(@RequestBody CsvSortColumnRequest request) {
        try {
            csvService.sortByColumn(request);
        }catch (Exception e){
            log.error("Sort CSV file by column has been failed {}",request,e);
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return ResponseEntity.ok().build();
    }




}

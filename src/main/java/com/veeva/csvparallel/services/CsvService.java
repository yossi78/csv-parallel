package com.veeva.csvparallel.services;
import com.veeva.csvparallel.dto.CsvSortColumnRequest;
import com.veeva.csvparallel.files.ExternalMergeSortService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.IOException;




@Service
@Slf4j
public class CsvService {


    private ExternalMergeSortService externalMergeSortService;

    @Autowired
    public CsvService(ExternalMergeSortService externalMergeSortService) {
        this.externalMergeSortService = externalMergeSortService;
    }

    public void sortByColumn(CsvSortColumnRequest request) throws IOException {
        externalMergeSortService.setFilePath(request.getFilePath());
        externalMergeSortService.setCompareIndex(request.getCompareIndex());
        externalMergeSortService.setMaxLineRead(request.getMaxLineRead());
        log.info("Start sort the file: "+ request.getFilePath());
        externalMergeSortService.externalMerge();
    }



}

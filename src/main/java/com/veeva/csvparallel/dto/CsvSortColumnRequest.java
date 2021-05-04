package com.veeva.csvparallel.dto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;



@Data
@AllArgsConstructor
@NoArgsConstructor
public class CsvSortColumnRequest {

    private String filePath;

    private Integer compareIndex;

    private Integer maxLineRead;

}

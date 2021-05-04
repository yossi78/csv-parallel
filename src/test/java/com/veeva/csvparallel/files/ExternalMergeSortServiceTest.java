package com.veeva.csvparallel.files;
import com.veeva.csvparallel.dao.Product;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.io.*;
import java.util.ArrayList;
import java.util.List;


public class ExternalMergeSortServiceTest {

    private ExternalMergeSortService externalMergeSortService;
    private String sourceFilePath ="src\\test\\resources\\SOURCE__100_ROWS.csv";
    private String actualFilePath ="src\\test\\resources\\SOURCE__100_ROWS.csv_sorted.csv";
    private String expectedFilePath="src\\test\\resources\\EXPECTED__100_ROWS.csv";
    private Integer compareIndex=1;
    private Integer maxLineRead=20;



    public ExternalMergeSortServiceTest(){
        this.externalMergeSortService =new ExternalMergeSortService(sourceFilePath,compareIndex,maxLineRead);
    }

    @Test
    public void externalMergeTest() throws Exception {
        List<Product> acualList=new ArrayList<>();
        List<Product> expectedList=new ArrayList<>();
        externalMergeSortService.externalMerge();
        acualList=readCsvToProductList(actualFilePath,acualList);
        expectedList=readCsvToProductList(expectedFilePath,expectedList);
        Assert.assertTrue(compareLists(expectedList,acualList));
    }

    @Test
    public void externalMergeNegativeTest() throws Exception {
        List<Product> acualList=new ArrayList<>();
        List<Product> expectedList=new ArrayList<>();
        externalMergeSortService.externalMerge();
        acualList=readCsvToProductList(actualFilePath,acualList);
        expectedList=readCsvToProductList(expectedFilePath,expectedList);
        acualList.get(0).setLine("Not equalls line");
        Assert.assertTrue(!compareLists(expectedList,acualList));
    }


    private Boolean compareLists(List<Product> expectedList,List<Product> actualList){
        for(int i=0;i<expectedList.size();i++){
            if(!expectedList.get(i).equals(actualList.get(i))){
                return false;
            }
        }
        return true;
    }


    private void initExternalMergeTest(){
        ReflectionTestUtils.setField(externalMergeSortService, "fileName", sourceFilePath);
        ReflectionTestUtils.setField(externalMergeSortService, "compareIndex", 1);
        ReflectionTestUtils.setField(externalMergeSortService, "maxLineRead", 5);
    }


    private  List<Product> readCsvToProductList(String filePath, List<Product> list) throws Exception {
        File file=new File(filePath);
        FileReader reader =new FileReader(file);
        BufferedReader br=new BufferedReader(reader);
        String line;
        String columns=line=br.readLine();
        while((line=br.readLine())!=null){
            Product currentProduct = new Product(line,compareIndex);
            list.add(currentProduct);
        }
        br.close();
        reader.close();
        return list;
    }


}

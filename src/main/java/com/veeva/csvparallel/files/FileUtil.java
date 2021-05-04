package com.veeva.csvparallel.files;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.veeva.csvparallel.dao.Product;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;



@Slf4j
public class FileUtil {



    public  static synchronized List<Product> readChunkOfLineSortAndWriteItToTempFile(BufferedReader br,AtomicInteger fileIndex,Integer maxLineRead, Integer compareIndex,String filePath) throws IOException, InterruptedException {
        List<Product> listOfLines =readChunkOfLines(br,maxLineRead,compareIndex);
        if(listOfLines.size()==0){
            log.info("The file has been read completely");
            return listOfLines;
        }
        Collections.sort(listOfLines, new Product(compareIndex));
        writeChunkFile(fileIndex,listOfLines,filePath);
        return listOfLines;
    }


    // THE METHOD READ X LINES FROM FILE AND RETURN THE LAST LINE WHICH WILL BE NULL IN CASE OF FINISHING READING
    public static List<Product> readChunkOfLines(BufferedReader br,Integer maxLineRead, Integer compareIndex) throws IOException {
        List<Product> listOfLines=new ArrayList<>();
        String line = null;
        Integer readLinesCounter=0;
        for (;readLinesCounter < maxLineRead; readLinesCounter++) {
            line = br.readLine();
            if (line == null) {
                return listOfLines;
            } else if (line.trim().equals("")) {
                continue;
            } else {
                listOfLines.add(new Product(line, compareIndex));
            }
        }
        log.info("Finish to read chunk");
        return listOfLines;
    }


    public static void writeChunkFile(AtomicInteger fileIndex, List<Product> listOfLines,String filePath) throws IOException {
        fileIndex.addAndGet(1);
        FileWriter fw = new FileWriter(generateFile(filePath,fileIndex.get()));
        BufferedWriter bw = new BufferedWriter(fw);
        for (int i = 0; i < listOfLines.size(); i++) {
            bw.append(listOfLines.get(i).line + "\n");
        }
        bw.close();
        listOfLines.clear();
    }


    //  GENERATE TEMP FILES
    public static File generateFile(String filePath,int index) {
        File file = new File(generateFileName(filePath,index));
        return file;
    }


    public static String generateFileName(String filePath,int index) {
        return filePath + "_" + "chunk" + "_" + index;
    }


    public static String readLine(BufferedReader br) throws IOException {
        String line=br.readLine();
        return line;
    }

    public static Integer getNumberOfLinesFromFile(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        Long lines = Files.lines(path).count();
        Integer numberOfLinesFromFile=lines.intValue();
        return numberOfLinesFromFile;
    }


    public FileUtil() {
    }

    public static Product fetchSmallestLine(int numberOfLines, int maxLineRead, String filePath, int compareIndex) throws IOException {
        String fileIndexPrefix="_chunk_";
        String cacheFilePath=filePath+"_cache";
        AtomicInteger fileStartIndex=new AtomicInteger(0);
        int fileNumber=numberOfLines/maxLineRead;
        int chunkNumber=fileNumber<=maxLineRead ? fileNumber : maxLineRead;
        Product smallestProduct=null;
        while(true) {
            List<BufferedReader> brList = FileUtil.generateBufferReaderList(filePath,fileIndexPrefix,fileStartIndex,fileStartIndex.get()+chunkNumber,fileNumber);
            List<Product> productList= FileUtil.readFirstLineFromEachFile(brList,compareIndex);
            if(productList.isEmpty()){
                return smallestProduct;
            }
            Collections.sort(productList, new Product(compareIndex));
            writeAdditionalListToFileAsJsons(cacheFilePath,productList,maxLineRead);
            Product smallestCacheProduct= FileUtil.fetchSmallestLineFromCacheFile(cacheFilePath,maxLineRead,compareIndex);
            smallestProduct=compareSmallestProductVsSmallestProductFromCache(smallestProduct,smallestCacheProduct,cacheFilePath,maxLineRead,compareIndex);
        }

    }


    private static Product compareSmallestProductVsSmallestProductFromCache(Product smallestProduct, Product smallestCacheProduct, String cacheFilePath, int maxLineRead, int compareIndex) throws IOException {
        if(smallestProduct==null || smallestProduct.compare(smallestProduct,smallestCacheProduct)>0){
            smallestProduct=smallestCacheProduct;
            removeJsonItemFromFile(cacheFilePath,maxLineRead,smallestCacheProduct);
        }
        return smallestProduct;
    }




    private static Product compareSmallestProductVsSmallestProductFromChunk(Product smallestProduct, Product smallestProductFromChunkFiles, List<Product> productList, String cacheFilePath,int maxLineRead) throws IOException {
        if(smallestProduct!=null && smallestProduct.compare(smallestProduct,smallestProductFromChunkFiles)>0){
            return updateSmallestProduct (smallestProduct,smallestProductFromChunkFiles,productList,cacheFilePath,maxLineRead);
        }
        if(smallestProduct==null){
            smallestProduct=new Product(smallestProductFromChunkFiles);
            productList.remove(smallestProductFromChunkFiles);
        }
        FileUtil.writeAdditionalListToFileAsJsons(cacheFilePath,productList,maxLineRead);
        productList.clear();
        return smallestProduct;
    }


    private static Product updateSmallestProduct(Product smallestProduct, Product smallestProductFromChunkFiles, List<Product> productList, String cacheFilePath,int maxLineRead) throws IOException {
        Product originSmallestProduct= new Product(smallestProduct);
        smallestProduct=smallestProductFromChunkFiles;
        productList.remove(smallestProductFromChunkFiles);
        productList.add(originSmallestProduct);
        FileUtil.writeAdditionalListToFileAsJsons(cacheFilePath,productList,maxLineRead);
        productList.clear();
        return smallestProduct;
    }


    private static void removeJsonItemFromFile(String filePath, int maxLineRead, Product exceptionProduct) throws IOException {
        String newFilePath=filePath+"_new";
        BufferedReader br=new BufferedReader(new FileReader(filePath));
        BufferedWriter bw=new BufferedWriter(new FileWriter(newFilePath));
        int linesNumber=getNumberOfLinesFromFile(filePath);
        for(int i=0;i<linesNumber;){
            for(int j=0;j<maxLineRead && i<linesNumber ;j++,i++){
                String objectStr=br.readLine();
                Product currentProduct= (Product) stringToObject(objectStr,Product.class);
                if(currentProduct.equals(exceptionProduct)){
                    continue;
                }
                bw.append(objectStr+"\n");
            }
        }
        System.gc();
        bw.close();
        br.close();
        new File(new File(filePath).getCanonicalPath()).delete();
        new File(newFilePath).renameTo(new File(filePath));
    }


    private static List<Product> readFirstLineFromEachFile(List<BufferedReader> brList,int compareIndex) throws IOException {
        List<Product> productList=new ArrayList<>();
        String line=null;
        for(int i=0;i<brList.size();i++){
            line=brList.get(i).readLine();
            Product product=new Product(line,compareIndex,i);
            productList.add(product);
        }
        return productList;
    }




    private static List<BufferedReader> generateBufferReaderList(String filePath, String fileIndexPrefix, AtomicInteger fileStartIndex, int endIndex, int fileNumber) throws IOException {
        List<BufferedReader> brList=new ArrayList<>();
        for(int i=fileStartIndex.get();i<endIndex && i<fileNumber;i++){
            BufferedReader bufferedReader=new BufferedReader(new FileReader(filePath+fileIndexPrefix+i));
            fileStartIndex.addAndGet(1);
            brList.add(bufferedReader);
        }
        return brList;
    }


    private static Product fetchSmallestLineFromCacheFile(String cacheFilePath, int maxLineRead,int compareIndex) throws IOException {
        Product smallestFromCaheFile=null;
        if(!FileUtil.isFileExists(cacheFilePath)) return null;
        BufferedReader br=new BufferedReader(new FileReader(cacheFilePath));
        int linesNumber=getNumberOfLinesFromFile(cacheFilePath);
        for(int i=0;i<linesNumber;){
            for(int j=0;j<maxLineRead && i<linesNumber;j++,i++){
                String line=br.readLine();
                Product currentProduct= (Product) stringToObject(line,Product.class);
                smallestFromCaheFile=(Product) FileUtil.findSmallestItem(smallestFromCaheFile,currentProduct);
            }
        }
        br.close();
        return smallestFromCaheFile;
    }



    private static Comparator findSmallestItem(Comparator first,Comparator second) {
        if(first!=null && first.compare(first,second)<0){
            return first;
        }else if(second==null){
            return first;
        }
        return second;
    }



    //xxxx
    public static void writeAdditionalListToFileAsJsons(String filePath, List<Product> productList, int maxLineRead) throws IOException {
        if(writeToFileAsJsonsForTheFirstTime(filePath,productList)){
            return;
        }
        String newFilePath=filePath+"_new";
        BufferedReader br=new BufferedReader(new FileReader(filePath));
        BufferedWriter bw=new BufferedWriter(new FileWriter(newFilePath));
        while(true){
            List<Product> currentProducts = readJsonsFromFile(br,maxLineRead,Product.class);
            if(currentProducts==null || currentProducts.isEmpty()){
                writeListToFileAsJsons(bw,productList);
                System.gc();
                bw.close();
                br.close();
                new File(new File(filePath).getCanonicalPath()).delete();
                new File(newFilePath).renameTo(new File(filePath));
                return;
            }
            writeListToFileAsJsons(bw,currentProducts);
        }
    }

    private static boolean writeToFileAsJsonsForTheFirstTime(String filePath, List<Product> productList) throws IOException {
        if(!isFileExists(filePath)){
            BufferedWriter bwCache=new BufferedWriter(new FileWriter(filePath));
            writeListToFileAsJsons(bwCache,productList);
            bwCache.close();
            return true;
        }
        return false;
    }


    public static <T> List<T>  readJsonsFromFile(BufferedReader br, int maxLineRead, Class T) throws IOException {
        List<T> resultList=new ArrayList<>();
        for(int i=0;i<maxLineRead;i++){
            String line=br.readLine();
            if(line==null){
                return resultList;
            }else if(line.isEmpty()){
                continue;
            }
            T currentObject = (T) stringToObject(line,T);
            resultList.add(currentObject);
        }
        return resultList;
    }


    private static void writeListToFileAsJsons(BufferedWriter bw, List<Product> productList) throws IOException {
        for (int i = 0; i < productList.size(); i++) {
            bw.append(objectToString( productList.get(i)) + "\n");
        }
    }



    private static void overWriteListToFileAsJsons(String filePath, List<Product> productList) throws IOException {
        BufferedWriter bw=new BufferedWriter(new FileWriter(filePath));
        try {
            for (int i = 0; i < productList.size(); i++) {
                bw.append(objectToString(productList.get(i)) + "\n");
            }
        }finally {
            bw.close();
        }
    }



    private static boolean isFileExists(String filePath) {
        try {
            BufferedReader br=new BufferedReader(new FileReader(filePath));
        } catch (FileNotFoundException e) {
            return false;
        }
        return true;
    }


    public static String objectToString(Object object) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String objectStr=null;
        objectStr = mapper.writeValueAsString(object);
        return objectStr;
    }

    public static Object stringToObject(String str, Class T) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        Object result = mapper.readValue(str,T);
        return result;
    }


}

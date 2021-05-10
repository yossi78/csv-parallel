package com.veeva.csvparallel.files;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import com.veeva.csvparallel.dao.Product;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import static com.veeva.csvparallel.files.FileUtil.readChunkOfLineSortAndWriteItToTempFile;


@Slf4j
@Data
@Service
@NoArgsConstructor
public class ExternalMergeSortService {

    private String filePath;
    private int compareIndex;
    private int maxLineRead;
    private String CHUNK = "_chunk_";


    public ExternalMergeSortService(String filePath, int compareIndex, int maxLineRead) {
        this.filePath = filePath;
        this.compareIndex = compareIndex;
        this.maxLineRead =maxLineRead;
    }


    /**
     * This method will be responsible for reading file, and creating some sorted temporary files from that.
     * After that this will merge all this file in out put files
     */
    public void externalMerge() throws IOException, InterruptedException {
        AtomicInteger fileIndex = new AtomicInteger(-1);
        BufferedReader br =new BufferedReader(new FileReader(filePath));
        String columnsLine=br.readLine();
        Integer numberOfLines= FileUtil.getNumberOfLinesFromFile(filePath);
        IntStream fileIndexStream=IntStream.range(0,numberOfLines/maxLineRead);
        fileIndexStream.parallel().forEach(c->{
            try {
                readChunkOfLineSortAndWriteItToTempFile(br,fileIndex,maxLineRead,compareIndex,filePath);
            } catch (IOException | InterruptedException e) {
            }
        });
        mergeFiles(fileIndex.get(),columnsLine,numberOfLines,filePath);
    }


    private List<FileUtil> generateFileReaderUtilList(int size){
        List<FileUtil> fileUtils =new ArrayList<>();
        for(int i=0;i<size;i++){
            FileUtil fileUtil =new FileUtil();
            fileUtils.add(fileUtil);
        }
        return fileUtils;
    }



    // THE MOTHOD SORT THE X LINES AND THEN CREATE A NEW FILE OF CHUNK AND SAVE THOSE SORTED LINES
    private void writeChunkFile(AtomicInteger fileIndex, List<Product> listOfLines) throws IOException {
        fileIndex.addAndGet(1);
        FileWriter fw = new FileWriter(generateFile(fileIndex.get()));
        BufferedWriter bw = new BufferedWriter(fw);
        for (int i = 0; i < listOfLines.size(); i++) {
            bw.append(listOfLines.get(i).line + "\n");
        }
        bw.close();
        listOfLines.clear();
    }


    private String generateFileName(int index) {
        return this.filePath + CHUNK
                + index;
    }

    //  GENERATE TEMP FILES
    private File generateFile(int index) {
        File file = new File(generateFileName(index));
        file.deleteOnExit();
        return file;
    }



    //  THE METHOD READ LINES FROM ALL CHUNK TEMP FILES AND SORT THEM AND CREATE FINAL CSV FILE
    private void mergeFiles(int numOfFiles, String columnsLine,int numberOfLines,String filePath) throws IOException, InterruptedException {
        BufferedWriter bwFinal=new BufferedWriter(new FileWriter(filePath+"_sorted.csv"));
        Product smallestProduct= FileUtil.fetchSmallestLineAndRemoveIt(numberOfLines,maxLineRead,filePath,compareIndex);
        bwFinal.append(columnsLine);
        bwFinal.append(smallestProduct+"\n");
        while (true){
            int fileIndex=smallestProduct.getIndexForFileName();
            readNextLineComingAfterCurrentOneAndMoveItToCacheFile(filePath,fileIndex,numOfFiles,numberOfLines);
            smallestProduct=FileUtil.readSmallestJsonFromFileAndRemoveIt(filePath+"_cache",maxLineRead);
            if(smallestProduct==null){
                break;
            }
            bwFinal.append(smallestProduct.getLine()+"\n");
        }
        bwFinal.close();
    }

    private Product findSmallestFromChunkVsCache(String filePath, int fileIndex, int numOfFiles, int numberOfLines) throws IOException, InterruptedException {
        Product smallestFromChunk= readNextLineComingAfterCurrentOne(filePath,fileIndex,numOfFiles,numberOfLines);
        return null;
    }


    private Product readNextLineComingAfterCurrentOne(String filePath, int fileIndex, int numOfFiles, int numberOfLines) throws IOException, InterruptedException {
        Product product=null;
        product = FileUtil.readLineAndRemoveIt(filePath+CHUNK+fileIndex,fileIndex,maxLineRead,compareIndex);
        if(product==null){
            product=FileUtil.readLineFromAnyFileExceptParamOneAndRemoveIt(filePath+CHUNK,fileIndex,numOfFiles,maxLineRead,compareIndex);
        }
        return product;
    }







    private void readNextLineComingAfterCurrentOneAndMoveItToCacheFile(String filePath, int fileIndex, int numOfFiles, int numberOfLines) throws IOException, InterruptedException {
        Product product=null;
        product = FileUtil.readLineAndRemoveIt(filePath+CHUNK+fileIndex,fileIndex,maxLineRead,compareIndex);
        if(product==null) {
            product = FileUtil.readLineFromAnyFileExceptParamOneAndRemoveIt(filePath + CHUNK, fileIndex, numOfFiles, maxLineRead, compareIndex);
        }
        if(product!=null){
            List<Product> list = new ArrayList<>();
            list.add(product);
            FileUtil.writeAdditionalListToFileAsJsons(filePath + "_cache", list, maxLineRead);
        }
    }






    //  THE METHOD READ LINES FROM ALL CHUNK TEMP FILES AND SORT THEM AND CREATE FINAL CSV FILE
    private void OLD_mergeFiles(int numOfFiles, String columnsLine) throws IOException {
        List<BufferedReader> bufferedReaderList=new ArrayList<>();
        for (int index = 0; index <= numOfFiles; index++) {
            String fileName = generateFileName(index);
            bufferedReaderList.add(new BufferedReader(new FileReader(fileName)));
        }
        sortFilesAndWriteOutput(bufferedReaderList,columnsLine);
        for (int index = 0; index < bufferedReaderList.size(); index++) {
            bufferedReaderList.get(index).close();
        }
    }




    //  THE METHOD FETCH EVERY X FILES AND READ THEIR FIRST LINE THEN SORT AND WRITE INTO FINAL RESULT CSV FILE
    private void sortFilesAndWriteOutput(List<BufferedReader> listOfBufferedReader, String columnsLine) throws IOException {
        List<Product> listOfLinesfromAllFiles =readFirstLineFromEachFile(listOfBufferedReader);
        FileWriter fw = new FileWriter(this.filePath + "_sorted.csv");
        BufferedWriter bw = new BufferedWriter(fw);
        bw.append(columnsLine + "\n");
        while (true) {
            if (listOfLinesfromAllFiles.size() == 0) {
                break;
            }
            Collections.sort(listOfLinesfromAllFiles, new Product(compareIndex));
            int indexForFileName=writeFirstAndSmallestLine(bw,listOfLinesfromAllFiles);
            Boolean isReadLineSucceed = OLD_readNextLineComingAfterCurrentOne(listOfBufferedReader,listOfLinesfromAllFiles,indexForFileName);
            if(isReadLineSucceed){
                continue;
            }
            readLineAttemptFromOneOfTheFilesExceptIndexOne(listOfBufferedReader,listOfLinesfromAllFiles,indexForFileName);
        }
        closeBufferWriters(bw,fw);
    }


    //  THE METHOD FETCH EVERY X FILES AND READ THEIR FIRST LINE THEN SORT AND WRITE INTO FINAL RESULT CSV FILE
    private void OLD_sortFilesAndWriteOutput(List<BufferedReader> listOfBufferedReader, String columnsLine) throws IOException {
        List<Product> listOfLinesfromAllFiles =readFirstLineFromEachFile(listOfBufferedReader);
        FileWriter fw = new FileWriter(this.filePath + "_sorted.csv");
        BufferedWriter bw = new BufferedWriter(fw);
        bw.append(columnsLine + "\n");
        while (true) {
            if (listOfLinesfromAllFiles.size() == 0) {
                break;
            }
            Collections.sort(listOfLinesfromAllFiles, new Product(compareIndex));
            int indexForFileName=writeFirstAndSmallestLine(bw,listOfLinesfromAllFiles);
            Boolean isReadLineSucceed = OLD_readNextLineComingAfterCurrentOne(listOfBufferedReader,listOfLinesfromAllFiles,indexForFileName);
            if(isReadLineSucceed){
                continue;
            }
            readLineAttemptFromOneOfTheFilesExceptIndexOne(listOfBufferedReader,listOfLinesfromAllFiles,indexForFileName);
        }
        closeBufferWriters(bw,fw);
    }


    private Boolean OLD_readNextLineComingAfterCurrentOne(List<BufferedReader> listOfBufferedReader
            , List<Product> listOfLinesfromAllFiles, int indexForFileName) throws IOException {
        String line = listOfBufferedReader.get(indexForFileName).readLine();
        if (line != null) {
            listOfLinesfromAllFiles.add(new Product(line,compareIndex,indexForFileName));
            return true;
        }
        return false;
    }


    private List<Product> readFirstLineFromEachFile(List<BufferedReader> listOfBufferedReader) throws IOException {
        List<Product> listOfLinesfromAllFiles = new ArrayList<Product>();
        // READ FIRST LINE FROM EACH TEMP CHUNK FILE
        for (int index = 0; index < listOfBufferedReader.size(); index++) {
            String line = listOfBufferedReader.get(index).readLine();
            if (line != null) {
                listOfLinesfromAllFiles.add(new Product(line, compareIndex,index));
            }
        }
        return  listOfLinesfromAllFiles;
    }



    private int writeFirstAndSmallestLine( BufferedWriter bw,List<Product> listOfLinesfromAllFiles) throws IOException {
        Product product = listOfLinesfromAllFiles.get(0);
        bw.append(product.line + "\n");
        int indexForFileName = product.indexForFileName;
        listOfLinesfromAllFiles.remove(0);
        return indexForFileName;
    }


    private void readLineAttemptFromOneOfTheFilesExceptIndexOne(List<BufferedReader> listOfBufferedReader, List<Product> listOfLinesfromAllFiles, int indexForFileName) throws IOException {
        for (int index = 0; index < listOfBufferedReader.size(); index++) {
            if (index == indexForFileName) {
                continue;
            } else {
                String line = listOfBufferedReader.get(index).readLine();
                if (line != null) {
                    listOfLinesfromAllFiles.add(new Product(line,compareIndex, index));
                    return ;
                }
            }
        }
    }


    private void closeBufferWriters(BufferedWriter bw,FileWriter fw) throws IOException {
        bw.close();
    }

}
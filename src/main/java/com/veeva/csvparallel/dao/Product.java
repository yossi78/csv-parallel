package com.veeva.csvparallel.dao;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.opencsv.bean.CsvBindByName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Comparator;





@Data
@NoArgsConstructor
@AllArgsConstructor
public class Product  implements Comparator<Product> {

    public String line;
    public Class compareClass;
    public int compareIndex;
    public int indexForFileName;


    @JsonProperty("id")
    @CsvBindByName(column = "ID")
    private Long id;

    @JsonProperty("name")
    @CsvBindByName(column = "Name")
    private String name;

    @JsonProperty("department")
    @CsvBindByName(column = "Department")
    private String department;

    @JsonProperty("description")
    @CsvBindByName(column = "Description")
    private String description;

    @JsonProperty("country")
    @CsvBindByName(column = "Country")
    private String country;

    @JsonProperty("city")
    @CsvBindByName(column = "City")
    private String city;

    @JsonProperty("code")
    @CsvBindByName(column = "Code")
    private String code;

    @JsonProperty("networkConnectivity")
    @CsvBindByName(column = "Network Connectivity")
    private String networkConnectivity;

    @JsonProperty("productManager")
    @CsvBindByName(column = "Produce Manager")
    private String productManager;

    @JsonProperty("improvments")
    @CsvBindByName(column = "Improvments")
    private String improvments;


    public Product(String line) {
        this.line = line;
        init(line);
    }


    public Product(int compareIndex) {
        this.setCompareIndex(compareIndex);
        if (compareIndex == 0)
            this.compareClass = Long.class;
        else
            this.compareClass = String.class;
    }


    public Product(String line, int compareIndex) {
        this.line = line;
        this.compareIndex = compareIndex;
        init(line);
    }


    public Product(String line, int compareIndex, int indexForFileName) {
        this.line = line;
        this.compareIndex=compareIndex;
        this.indexForFileName = indexForFileName;
        init(line);
    }


    public Product(Product otherProduct){
        this.setCity(otherProduct.getCity());
        this.setCode(otherProduct.getCode());
        this.setCountry(otherProduct.getCountry());
        this.setDepartment(otherProduct.getDepartment());
        this.setDescription(otherProduct.getDescription());
        this.setImprovments(otherProduct.getImprovments());
        this.setId(otherProduct.getId());
        this.setLine(otherProduct.getLine());
        this.setName(otherProduct.getName());
        this.setProductManager(otherProduct.getProductManager());
        this.setIndexForFileName(otherProduct.getIndexForFileName());
        this.setNetworkConnectivity(otherProduct.getNetworkConnectivity());
        this.setCompareClass(otherProduct.getCompareClass());
        this.setCompareIndex(otherProduct.getCompareIndex());
    }


    private void init(String line){
        String[] columns = line.split(",");
        this.setId(Long.valueOf(columns[0]));
        this.setName(columns[1]);
        this.setDepartment(columns[2]);
        this.setDescription(columns[3]);
        this.setCountry(columns[4]);
        this.setCity(columns[5]);
        this.setCode(columns[6]);
        this.setNetworkConnectivity(columns[7]);
        this.setProductManager(columns[8]);
        this.setImprovments(columns[9]);

    }



    @Override
    public int compare(Product p1, Product p2) {
        if (p1.getCompareIndex() == 0) {
            return compareLongs(p1.getId(),p2.getId());
        } else if (p1.getCompareIndex() == 1) {
            return compareStrings(p1.getName().toLowerCase(),p2.getName().toLowerCase());
        } else if (p1.getCompareIndex() == 2) {
            return compareStrings(p1.getDepartment().toLowerCase(),p2.getDepartment().toLowerCase());
        } else if (p1.getCompareIndex() == 3) {
            return compareStrings(p1.getDescription().toLowerCase(),p2.getDescription().toLowerCase());
        } else if (p1.getCompareIndex() == 4) {
            return compareStrings(p1.getCountry().toLowerCase(),p2.getCountry().toLowerCase());
        } else if (p1.getCompareIndex() == 5) {
            return compareStrings(p1.getCity().toLowerCase(),p2.getCity().toLowerCase());
        } else if (p1.getCompareIndex() == 6) {
            return compareStrings(p1.getCode().toLowerCase(),p2.getCode().toLowerCase());
        } else if (p1.getCompareIndex() == 7) {
            return compareStrings(p1.getNetworkConnectivity().toLowerCase(),p2.getNetworkConnectivity().toLowerCase());
        } else if (p1.getCompareIndex() == 8) {
            return compareStrings(p1.getProductManager().toLowerCase(),p2.getProductManager().toLowerCase());
        } else if (p1.getCompareIndex() == 9) {
            return compareStrings(p1.getImprovments().toLowerCase(),p2.getImprovments().toLowerCase());
        } else
            return 0;

    }


    private int compareStrings(String str1,String str2){
        if(str1.equals(str2)){
            return 0;
        }
        if(str1.compareTo(str2)>0){
            return 1;
        }else{
            return -1;
        }
    }


    private int compareLongs(Long num1,Long num2){
        if(num1.equals(num2)){
            return 0;
        }
        if(num1>num2){
            return 1;
        }else{
            return -1;
        }
    }


    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        if (other == this)
            return true;
        if (this.getClass() != other.getClass())
            return false;
        Product otherMyClass = (Product) other;
        return otherMyClass.line.equals(this.line);
    }



    @Override
    public int hashCode() {
        return this.line.hashCode();
    }




}

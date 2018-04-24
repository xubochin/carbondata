package org.apache.carbondata.core.datamap;


import java.util.List;

public class IndexAttributes {
    public enum CompositeType{
        Geo3DPoint("Goe3DPoint"),
        GeoDDocValues("GeoDDocValuesField");
        private String name;
        CompositeType(String name){
            this.name = name;
        }
        public static CompositeType fromTypeName(String typeName){
            for(CompositeType type : CompositeType.values()){
                if(type.getTypeName().equals(typeName)){
                    return type;
                }
            }
            return null;
        }
        public String getTypeName(){
            return this.name;
        }
    }
    private String columnName;
    private List<String> subColumnNames;
    private List<Integer> subColumnIdxs;
    private CompositeType compositeType;
    private boolean stored;

    public IndexAttributes(String columnName, List<String> subColumnNames, String compositeType, boolean stored){
        this.columnName = columnName;
        this.subColumnNames = subColumnNames;
        this.compositeType = CompositeType.fromTypeName(compositeType);
        this.stored = stored;
    }


    public String getColumnName() {return columnName; }
    public List<String> getSubColumnNames() { return subColumnNames;}
    public List<Integer> getSubColumnIdxs() { return subColumnIdxs; }
    public boolean getStored() {return stored;}
    public void setSubColumnIdxs(List<Integer> subColumnIdxs){this.subColumnIdxs=subColumnIdxs;}

    public CompositeType getCompositeType() {
        return compositeType;
    }
}


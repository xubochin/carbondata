package org.apache.carbondata.datamap.lucene;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.dev.AbstractDataMapWriter;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.fgdatamap.AbstractFineGrainDataMap;
import org.apache.carbondata.core.datamap.dev.fgdatamap.AbstractFineGrainDataMapFactory;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.events.Event;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LuceneFineGrainDataMapFactory extends AbstractFineGrainDataMapFactory {
    /**
     * Logger
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(LuceneFineGrainDataMapFactory.class.getName());

    /**
     * table's index columns
     */
    private DataMapMeta dataMapMeta = null;

    /**
     * analyzer for lucene
     */
    private Analyzer analyzer = null;

    /**
     * index name
     */
    private DataMapSchema dataMapSchema = null;

    /**
     * table identifier
     */
    private AbsoluteTableIdentifier tableIdentifier = null;


    /**
     * Initialization of Datamap factory with the identifier and datamap name
     *
     * @param identifier
     * @param dataMapSchema
     */
    public void init(AbsoluteTableIdentifier identifier, DataMapSchema dataMapSchema) throws IOException {
        this.tableIdentifier = identifier;
        this.dataMapSchema = dataMapSchema;

        List <String> indexedColumns = LuceneCoarseGrainDataMapFactory.getIndexColumns(dataMapSchema);
        if (indexedColumns.size() == 0) {
            throw new IOException("no columns to write index");
        }

        /**
         * add optimizedOperations
         */
        List <ExpressionType> optimizedOperations = new ArrayList <ExpressionType>();
        optimizedOperations.add(ExpressionType.EQUALS);
        optimizedOperations.add(ExpressionType.GREATERTHAN);
        optimizedOperations.add(ExpressionType.GREATERTHAN_EQUALTO);
        optimizedOperations.add(ExpressionType.LESSTHAN);
        optimizedOperations.add(ExpressionType.LESSTHAN_EQUALTO);
        optimizedOperations.add(ExpressionType.NOT);
        this.dataMapMeta = new DataMapMeta(indexedColumns, optimizedOperations);

        /**
         * get analyzer, if failure , use default StandardAnalyzer
         */
        String strClassAnalyzer = LuceneCoarseGrainDataMapFactory.getIndexProperty(dataMapSchema, "index.analyzer",
                "org.apache.lucene.analysis.standard.StandardAnalyzer");
        try {
            Class classAnalyzer = Class.forName(strClassAnalyzer);
            analyzer = (Analyzer) classAnalyzer.newInstance();
            LOGGER.info("User analyzer : " + strClassAnalyzer);
        } catch (Exception e) {
            LOGGER.warn(String.format(
                    "failed to get instance of %s, detail is  %s", strClassAnalyzer, e.getMessage()));
            analyzer = new StandardAnalyzer();
            LOGGER.info("User default StandardAnalyzer");
        }
        LOGGER.info("success to init lucene coarse data map factory for data map  " + dataMapSchema.getDataMapName());
    }

    /**
     * Return a new write for this datamap
     *
     * @param segmentId
     * @param writeDirectoryPath
     */
    public AbstractDataMapWriter createWriter(String segmentId, String writeDirectoryPath) {
        LOGGER.info("lucene data write to temporary path " + writeDirectoryPath);
        return new LuceneDataMapWriter(tableIdentifier,
                dataMapSchema, segmentId, writeDirectoryPath, true);
    }

    /**
     * Get the datamap for segmentid
     *
     * @param segmentId
     */
    public List <AbstractFineGrainDataMap> getDataMaps(String segmentId) throws IOException {
        List <AbstractFineGrainDataMap> lstDataMap = new ArrayList <AbstractFineGrainDataMap>();
        AbstractFineGrainDataMap dataMap =
                new LuceneFineGrainDataMap(tableIdentifier, dataMapSchema, segmentId, analyzer);
        try {
            dataMap.init(
                    new DataMapModel(tableIdentifier.getTablePath()
                            + "/Fact/Part0/Segment_" + segmentId + File.separator + dataMapSchema.getDataMapName()));
        } catch (MemoryException e) {
            LOGGER.error("failed to get lucene datamap , detail is {}" + e.getMessage());
            return lstDataMap;
        }
        lstDataMap.add(dataMap);
        return lstDataMap;
    }

    /**
     * Get datamaps for distributable object.
     *
     * @param distributable
     */
    public List <AbstractFineGrainDataMap> getDataMaps(DataMapDistributable distributable) throws IOException {
        return getDataMaps(distributable.getSegmentId());
    }

    /**
     * Get datamap for distributable object.
     *
     * @param distributable
     */
    public DataMap getDataMap(DataMapDistributable distributable) {
        return new LuceneFineGrainDataMap(tableIdentifier, dataMapSchema, distributable.getSegmentId(), analyzer);
    }

    /**
     * Get all distributable objects of a segmentid
     *
     * @param segmentId
     * @return
     */
    public List <DataMapDistributable> toDistributable(String segmentId) {
        List <DataMapDistributable> lstDataMapDistribute = new ArrayList <DataMapDistributable>();
        DataMapDistributable luceneDataMapDistributable = new LuceneDataMapDistributable();
        luceneDataMapDistributable.setTablePath(tableIdentifier.getTablePath());
        luceneDataMapDistributable.setDataMapSchema(dataMapSchema);
        luceneDataMapDistributable.setSegmentId(segmentId);
        lstDataMapDistribute.add(luceneDataMapDistributable);
        return lstDataMapDistribute;
    }

    /**
     * @param event
     */
    public void fireEvent(Event event) {

    }

    /**
     * Clears datamap of the segment
     *
     * @param segmentId
     */
    public void clear(String segmentId) {

    }

    /**
     * Clear all datamaps from memory
     */
    public void clear() {

    }

    /**
     * Return metadata of this datamap
     */
    public DataMapMeta getMeta() {
        return dataMapMeta;
    }

}

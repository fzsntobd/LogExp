package transformer.mr.inbound;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;

import common.GlobalConstants;
import transformer.model.dim.StatsInboundDimension;
import transformer.model.dim.base.BaseDimension;
import transformer.model.value.BaseStatsValueWritable;
import transformer.model.value.reduce.InboundReduceValue;
import transformer.mr.IOutputCollector;
import transformer.service.IDimensionConverter;

public class InboundCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        StatsInboundDimension inboundDimension = (StatsInboundDimension) key;
        InboundReduceValue inboundReduceValue = (InboundReduceValue) value;

        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(inboundDimension.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(inboundDimension.getStatsCommon().getDate()));
        pstmt.setInt(++i, inboundDimension.getInbound().getId()); // 直接设置，在mapper类中已经设置
        pstmt.setInt(++i, inboundReduceValue.getUvs());
        pstmt.setInt(++i, inboundReduceValue.getVisit());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, inboundReduceValue.getUvs());
        pstmt.setInt(++i, inboundReduceValue.getVisit());

        pstmt.addBatch();
    }
}

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class PutExample {
	static Configuration hbaseConfiguration = HBaseConfiguration.create();
	static {
		hbaseConfiguration.addResource("hbase-site.xml");
	}

	/**
	 * 创建表
	 * 
	 * @param tablename
	 *            表名
	 * @param columnFamily
	 *            列族
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 * @throws MasterNotRunningException
	 */
	public static void CreateTable(String tablename, String columnFamily)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		HBaseAdmin admin = new HBaseAdmin(hbaseConfiguration);
		if (admin.tableExists(tablename)) {// 如果表已经存在
			System.out.println(tablename + "表已经存在!");
		} else {
			TableName tableName = TableName.valueOf(tablename);
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
			System.out.println(tablename + "表已经成功创建!");
		}
	}

	/**
	 * 创建一个新的put
	 * 
	 * @param row
	 *            行健
	 * @param columnFamily
	 *            列群
	 * @param column
	 *            列名
	 * @param data
	 *            数据
	 * @return
	 * @throws IOException
	 */
	public static Put getPut(String row, String columnFamily, String column,
			String data) throws IOException {
		Put put = new Put(Bytes.toBytes(row));
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
				Bytes.toBytes(data));
		System.out.println("Put生成成功： '" + row + "','" + columnFamily + ":"
				+ column + "','" + data + "'");
		return put;
	}

	public static Delete getDelete(String row, String columnFamily,
			String column) throws IOException {
		Delete delete = null;
		if (row != null) {
			delete = new Delete(Bytes.toBytes(row));
		}
		if (columnFamily != null) {
			if (column != null) {
				delete.deleteColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
			}else{
				delete.deleteFamily(Bytes.toBytes(columnFamily));
			}
		}
		return delete;
	}

	/**
	 * 向表中插入一条数据
	 * 
	 * @param tableName
	 * @param put
	 * @throws IOException
	 */
	public static void PutData(String tableName, Put put) throws IOException {
		if (put != null) {
			HTable table = new HTable(hbaseConfiguration, tableName);
			table.put(put);
			System.out.println("数据插入成功！");
		} else {
			System.out.println("数据插入失败！");
		}
	}

	/**
	 * 获取指定行的所有数据
	 * 
	 * @param tableName
	 *            表名
	 * @param row
	 *            行键key
	 * @param columnFamily
	 *            列族
	 * @param column
	 *            列名
	 * @throws IOException
	 */
	public static String GetData(String tableName, String row,
			String columnFamily, String column) throws IOException {
		HTable table = new HTable(hbaseConfiguration, tableName);
		Get get = new Get(Bytes.toBytes(row));
		// get.addColumn(family, qualifier);//指定get取得那一列的数据
		// get.addFamily(family);//指定取得某一个列族数据
		Result result = table.get(get);
		byte[] rb = result.getValue(Bytes.toBytes(columnFamily),
				Bytes.toBytes(column));
		String value = new String(rb, "UTF-8");
		System.out.println(value);
		return value;
	}

	/**
	 * 批量获取指定行的所有数据
	 * 
	 * @param tableName
	 *            表名
	 * @param gets
	 *            GET集合
	 * @return
	 * @throws IOException
	 */
	public static Result[] ListGetData(String tableName, List<Get> gets)
			throws IOException {
		HTable table = new HTable(hbaseConfiguration, tableName);
		Result[] results = table.get(gets);
		return results;
	}

	/**
	 * 获取指定列的所有数据
	 * 
	 * @param tableName
	 *            表名
	 * @param row
	 *            行键key
	 * @param columnFamily
	 *            列族
	 * @param column
	 *            列名
	 * @throws IOException
	 */
	public static List<KeyValue> GetDataByColumnn(String tableName, String row,
			String columnFamily, String column) throws IOException {
		HTable table = new HTable(hbaseConfiguration, tableName);
		Get get = new Get(Bytes.toBytes(row));
		// get.addColumn(family, qualifier);//指定get取得那一列的数据
		// get.addFamily(family);//指定取得某一个列族数据
		Result result = table.get(get);
		List<KeyValue> columnList = result.getColumn(
				Bytes.toBytes(columnFamily), Bytes.toBytes(column));
		for (KeyValue re : columnList) {
			System.out.println(new String(re.getValue(), "UTF-8"));
		}
		return columnList;
	}

	/**
	 * 获取指定表的所有数据
	 * 
	 * @param tableName
	 *            表名
	 * @throws IOException
	 */
	public static void ScanAll(String tableName) throws IOException {
		System.out.println("开始扫描全表......");
		HTable table = new HTable(hbaseConfiguration, tableName);
		Scan scan = new Scan();
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner) {
			List<Cell> cells = result.listCells();
			for (Cell cell : cells) {
				byte[] rb = cell.getValueArray();
				String row = new String(result.getRow(), "UTF-8");
				String family = new String(CellUtil.cloneFamily(cell), "UTF-8");
				String qualifier = new String(CellUtil.cloneQualifier(cell),
						"UTF-8");
				String value = new String(CellUtil.cloneValue(cell), "UTF-8");
				System.out.println("[row:" + row + "],[family:" + family
						+ "],[qualifier:" + qualifier + "],[value:" + value
						+ "]");
			}
		}
	}

	/**
	 * 批量进行put插入
	 * 
	 * @param tableName
	 *            表名
	 * @param puts
	 *            列表
	 * @throws IOException
	 */
	public static void ListPut(String tableName, List<Put> puts)
			throws IOException {
		System.out.println("数据开始批量插入......");
		HTable table = new HTable(hbaseConfiguration, tableName);
		if (puts.size() != 0) {
			table.setAutoFlush(false);
			table.put(puts);
			table.flushCommits();
			System.out.println("数据批量插入成功！");
		} else {
			System.out.println("数据批量插入失败！");
		}
	}

	/**
	 * 原子操作：检查并put： 在put之前进行检查，检查通过才Put，否则就返回false
	 * 注意：该方法只能检查和修改同一行数据，若检查的数据和put的数据不是同一个单元格
	 * ，则会抛出org.apache.hadoop.hbase.DoNotRetryIOException异常
	 * 
	 * @param tableName
	 *            ：表名
	 * @param row
	 *            ：待检查行健
	 * @param columnFamily
	 *            ：待检查列族
	 * @param column
	 *            ：待检查列名
	 * @param data
	 *            ：待检查数据，如果需要判断是否不存在，则直接传递null
	 * @param put
	 *            ：需要put的对象
	 * @return
	 * @throws IOException
	 */
	public static boolean checkPut(String tableName, byte[] row,
			byte[] columnFamily, byte[] column, byte[] data, Put put)
			throws IOException {
		HTable table = new HTable(hbaseConfiguration, tableName);
		boolean check = table.checkAndPut(row, columnFamily, column, data, put);
		return check;
	}

	/**
	 * 从表中删除一条数据
	 * 
	 * @param tableName
	 * @param put
	 * @throws IOException
	 */
	public static void DeleteData(String tableName, Delete delete) throws IOException {
		if (delete != null) {
			HTable table = new HTable(hbaseConfiguration, tableName);
			table.delete(delete);;
			System.out.println("数据删除成功！");
		} else {
			System.out.println("数据删除失败！");
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {

			PutExample.CreateTable("userinfo", "vio1");
			PutExample.PutData("userinfo",
					getPut("row1", "vio1", "col1", "Hello1"));
			PutExample.PutData("userinfo",
					getPut("row1", "vio1", "col2", "Hello2"));
			PutExample.PutData("userinfo",
					getPut("row1", "vio1", "col3", "Hello3"));
			PutExample.PutData("userinfo",
					getPut("row1", "vio1", "col4", "Hello4"));
			PutExample.PutData("userinfo",
					getPut("row2", "vio1", "col1", "World1"));
			PutExample.PutData("userinfo",
					getPut("row2", "vio1", "col2", "World2"));
			PutExample.PutData("userinfo",
					getPut("row2", "vio1", "col3", "World3"));
			PutExample.PutData("userinfo",
					getPut("row2", "vio1", "col4", "World4"));
			PutExample.PutData("userinfo",
					getPut("row2", "vio1", "col5", "World5"));

			// PutExample.GetData("userinfo", "row1", "vio1", "col1");
			// PutExample.GetData("userinfo", "row1", "vio1", "col2");

			// boolean check =
			// checkPut("userinfo",Bytes.toBytes("row1"),Bytes.toBytes("vio1"),Bytes.toBytes("col1"),
			// Bytes.toBytes("驾驶车辆违法信息2："), getPut("row1", "vio1", "col1",
			// "驾驶车辆违法信息2："));
			// System.out.println(check);
			// PutExample.PutData("userinfo",
			// getPut("row1", "baseinfo", "vio1", "驾驶车辆违法信息2："));
			// PutExample.PutData("userinfo",
			// getPut("row5", "baseinfo", "column3", "这是一个列值"));
			// PutExample.GetData("userinfo", "row2","baseinfo","vio2");
			// PutExample.GetData("userinfo", "row3", "baseinfo", "column3");
			//
			// List<Put> puts=new ArrayList<Put>();
			// puts.add(getPut("row6", "baseinfo", "vio2", "驾驶车辆违法信息3："));
			// puts.add(getPut("row7", "baseinfo", "vio2", "驾驶车辆违法信息4："));
			// puts.add(getPut("row8", "baseinfo", "vio2", "驾驶车辆违法信息5："));
			// puts.add(getPut("row9", "baseinfo", "vio2", "驾驶车辆违法信息6："));
			// puts.add(getPut("row10", "baseinfo", "vio2", "驾驶车辆违法信息7："));
			// puts.add(getPut("row11", "baseinfo", "vio2", "驾驶车辆违法信息7："));
			// ListPut("userinfo", puts);

			// 批量查找
			// List<Get> gets = new ArrayList<Get>();
			// gets.add(new Get(Bytes.toBytes("row1")));
			// gets.add(new Get(Bytes.toBytes("row2")));
			// Result[] results = ListGetData("userinfo", gets);
			// for (Result result : results) {
			// String row = Bytes.toString(result.getRow());
			// System.out.println("Row:" + row);
			// if (result.containsColumn(Bytes.toBytes("vio1"),
			// Bytes.toBytes("col1"))) {
			// System.out.println("col1:"
			// + Bytes.toString(result.getValue(Bytes.toBytes("vio1"),
			// Bytes.toBytes("col1"))));
			// }
			// if (result.containsColumn(Bytes.toBytes("vio1"),
			// Bytes.toBytes("col2"))) {
			// System.out.println("col2:"
			// + Bytes.toString(result.getValue(Bytes.toBytes("vio1"),
			// Bytes.toBytes("col2"))));
			// }
			// if (result.containsColumn(Bytes.toBytes("vio1"),
			// Bytes.toBytes("col3"))) {
			// System.out.println("col3:"
			// + Bytes.toString(result.getValue(Bytes.toBytes("vio1"),
			// Bytes.toBytes("col3"))));
			// }
			// if (result.containsColumn(Bytes.toBytes("vio1"),
			// Bytes.toBytes("col4"))) {
			// System.out.println("col4:"
			// + Bytes.toString(result.getValue(Bytes.toBytes("vio1"),
			// Bytes.toBytes("col4"))));
			// }
			//
			// }
			// for (Result result : results) {
			// for (KeyValue kv : result.raw()) {
			// System.out.println("Row: "+Bytes.toString(kv.getRow())+",Value："+Bytes.toString(kv.getValue()));
			// }
			// }
			PutExample.ScanAll("userinfo");
			PutExample.DeleteData("userinfo", getDelete("row1","vio1","col1"));
			PutExample.ScanAll("userinfo");

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}

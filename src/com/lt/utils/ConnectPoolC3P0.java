package com.lt.utils;

import java.beans.PropertyVetoException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.lt.utils.DateUtil;
import com.mchange.v2.c3p0.ComboPooledDataSource;

public class ConnectPoolC3P0 {

	/**
	 * 连接池使用
	 */
	private ComboPooledDataSource dataSource;
	private static ConnectPoolC3P0 conn;
	private String drive;
	static Logger log = Logger.getLogger("KafkaLogger");
	 /**
	  * 获取连接池实例
	  * @return
	  */
	 public static ConnectPoolC3P0 getInstance(){
		 if(conn == null){
			 conn = new ConnectPoolC3P0();
		 }
		 return conn;
	 }
	// 初始化连接池
	private ConnectPoolC3P0(){
		 try {
			String path = System.getProperty("user.dir") + "/src/dbpool.properties";
			 FileInputStream is = new FileInputStream(path);
			 Properties props = new Properties();
			 props.load(is);
			 String driverClassName = props.getProperty("dbdriver").trim();
			 String username = props.getProperty("dbuser").trim();
			 String password = props.getProperty("dbpassword").trim();
			 String url = props.getProperty("dburl").trim();
			dataSource = new ComboPooledDataSource();
			dataSource.setDriverClass(driverClassName);
			dataSource.setJdbcUrl(url);
			dataSource.setUser(username);
			dataSource.setPassword(password);
			
			drive = dataSource.getDriverClass();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.info(e);
			e.printStackTrace();
		}  
	}
		
	/**
	 * 获取连接
	 */
	public Connection getConnection (){
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return conn;
	}
	 
	/**
	 * 释放连接
	 * @param con
	 */
	public void freeConnection(Connection con) {
		try {
			if(null != con){
				con.close();
				con = null;
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if (null != con) {
				try {
					con.close();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				con = null;
			}
		}
	}
		
	/**
	 * 释放Statement资源
	 * @param statement
	 */
	public void freeStatement(Statement statement){
        try {
        	if(null != statement){
                statement.close();
                statement = null;
        	}
        } catch(Exception e) {
        	System.out.println(e.getMessage());
        } finally {
        	if (null !=statement) {
        		try {
                    statement.close();
				}catch(Exception e) {
					System.out.println(e.getMessage());
				}
        		statement = null;
        	}
        }
	}
		
	/**
	 * 释放查询结果
	 * @param statement
	 */
	public void freeResultSet(ResultSet rs){
        try {
              if(null != rs){
                rs.close();
                rs = null;
              }
        } catch(Exception e) {
    		System.out.println(e.getMessage());
        } finally {
        	if (null !=rs) {
        		try {
                  rs.close();
				}catch(Exception e) {
					System.out.println(e.getMessage());
				}
        		rs = null;
        	}
        }
	}
	 
	/**
	 * 关闭hsqldb连接
	 * @param con
	 * @throws SQLException
	 */
	public void freeHsqldb(Connection con) {
		try{
			// hsqldb 需要关闭数据库
			if(drive.contains("hsqldb")){
				Statement st = con.createStatement();
				st.executeUpdate("  SHUTDOWN  ");
				st.close();
				st = null;
			}
		}catch(SQLException e){
			e.printStackTrace();
		}
	}
	
	/***
	 * 设置sql语句中的问号
	 * @param ps
	 * @param param
	 * @throws SQLException
	 */
	private  void setParam(PreparedStatement ps,List<Object> param) throws SQLException{
		int seq = 1;
		// 如果为null,则表示没有参数
		if (param != null) {
			for (Object elem : param) {
				// String 类型
				if (elem instanceof String) {
					ps.setString(seq, (String) elem);
					// Long 类型
				} else if (elem instanceof Long) {
					ps.setLong(seq, (Long) elem);
					// Double 类型
				} else if (elem instanceof Double) {
					ps.setDouble(seq, (Double) elem);
					// sql 日期类型
				} else if (elem instanceof Float) {
					ps.setFloat(seq, (Float) elem);
					// Integer 整形
				} else if (elem instanceof Integer) {
					ps.setInt(seq, (Integer) elem);
					// Timestamp 类型
				} else if (elem instanceof Timestamp) {
					ps.setTimestamp(seq, (Timestamp) elem);
				} else if (elem instanceof Date) {
					ps.setDate(seq, (Date) elem);
					// util 日期类型
				} else if (elem instanceof java.util.Date) {
					java.util.Date tmp = (java.util.Date) elem;
					ps.setDate(seq, new Date(tmp.getTime()));
					// Float 浮点类型
				} else {
					ps.setString(seq, (String) elem);
				}
				seq++;
			}
		}
	}
	
    /**
     * 根据结果集中的数据类型 转换成相应String 类型
     * @throws SQLException 
     */
    private String parseResultSet(ResultSet rs, ResultSetMetaData rsmd,
			int type, int i) throws SQLException {
		String resStr = "";
		switch (type) {
			case Types.VARCHAR:
				resStr = rs.getString(i + 1);
				break;
			case Types.NUMERIC:
				NumberFormat nf = NumberFormat.getInstance();
				nf.setGroupingUsed(false);
				int scale = rsmd.getScale(i + 1);
				if (scale == 0) {
					resStr = nf.format(rs.getLong(i + 1));
				} else {
					nf.setMinimumFractionDigits(2);
					resStr = nf.format(rs.getDouble(i + 1));
				}
				break;
			case Types.CHAR:
				resStr = rs.getString(i + 1);
				break;
			case Types.FLOAT:
				resStr = String.valueOf(rs.getFloat(i + 1));
				break;
			case Types.DOUBLE:
				resStr = String.valueOf(rs.getDouble(i + 1));
				break;
			case Types.DATE:
				resStr = DateUtil.getStringDate(rs.getDate(i + 1));
				break;
			case Types.TIMESTAMP:
				resStr = DateUtil.getStringTimestamp(rs.getTimestamp(i + 1));
				break;
			default:
				resStr = rs.getString(i + 1);
		}

		return resStr;
	}
	
	/**
	 * 返回数组格式
	 * @param sql
	 * @param param
	 * @return
	 */
	public List<String[]> queryForArray(String sql, List<Object> param) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		ResultSetMetaData rsmd = null;
		List<String[]> list = new ArrayList<String[]>();
		try {
			// 获取一个连接
			conn = this.getConnection();
			// 执行预备语句
			ps = conn.prepareStatement(sql);
			// 设置sql 语句中的参数
			setParam(ps,param);
			// 查询操作
			rs = ps.executeQuery();
			// 获取结果集列信息
			rsmd = rs.getMetaData();
			// 获取总列数
			rsmd = rs.getMetaData();
			int columCount = rsmd.getColumnCount();
			while (rs.next()) {
				String[] resStr = new String[columCount];
				for (int i = 0; i < columCount; i++) {
					int type = rsmd.getColumnType(i + 1);
					resStr[i] = parseResultSet(rs,rsmd,type,i);
				}
				list.add(resStr);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			// 关闭连接 释放资源
			this.freeResultSet(rs);
			this.freeStatement(ps);
			this.freeConnection(conn);
		}

		return list;
	}

	/**
	 * 修改，删除，添加使用
	 * @param sql
	 * @param param
	 * @return
	 */
	public int execute(String sql, List<Object> param) {
		// 处理结果
		int result = 0;
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			// 获取一个连接
			conn = this.getConnection();
			// 执行预备语句
			ps = conn.prepareStatement(sql);
			// 设置sql 语句中的参数
			setParam(ps,param);
			// 执行操作
			result = ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			// 关闭连接 释放资源
			this.freeStatement(ps);
			this.freeConnection(conn);
		}

		return result;
	}

	/**
	 * 用于事务控制，修改，删除，添加 使用
	 * @param sql
	 * @param param
	 * @return
	 */
	public int execute(Connection conn, String sql, List<Object> param) {
		// 处理结果
		int result = 0;
		PreparedStatement ps = null;
		try {
			// 执行预备语句
			ps = conn.prepareStatement(sql);
			// 设置sql 语句中的参数
			setParam(ps,param);
			// 执行操作
			result = ps.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			this.freeStatement(ps);
		}

		return result;
	}

	
	/**
	 * 添加使用 返回id
	 * @param sql
	 * @param param
	 * @return
	 */
	public int insertForKey(String sql,List<Object> param) {
		
		
		int result = 0;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs=null;
		try {
			// 获取一个连接
			conn = this.getConnection();
			ps = conn.prepareStatement(sql,PreparedStatement.RETURN_GENERATED_KEYS);
			this.setParam(ps,param);
			ps.executeUpdate();
			rs = ps.getGeneratedKeys(); 
			if (rs.next()) {
				result = rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			// 关闭连接 释放资源
			this.freeStatement(ps);
			this.freeConnection(conn);
		}
		
		 
		return result;
	}
	/**
	 * 用于事务控制，修改，删除，添加 使用，返回自动生成的主键
	 * @param conn
	 * @param sql
	 * @param param
	 * @return
	 */
	public int executeForKey(Connection conn, String sql, List<Object> param) {
		int result = 0;
		PreparedStatement ps = null;
		ResultSet rs=null;
		try {
			ps = conn.prepareStatement(sql,PreparedStatement.RETURN_GENERATED_KEYS);
			this.setParam(ps,param);
			ps.executeUpdate();
			rs = ps.getGeneratedKeys(); 
			if (rs.next()) {
				result = rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			this.freeResultSet(rs);
			this.freeStatement(ps);
		}
		
		return result;
	}
	
	/**
	 * 查询表中的记录数，返回数量
	 * @param sql
	 * @param param
	 * @return
	 */
	public int count(String sql, List<Object> param) {
		// 处理结果
		int result = 0;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			// 获取一个连接
			conn = this.getConnection();
			// 执行预备语句
			ps = conn.prepareStatement(sql);
			// 设置sql 语句中的参数
			setParam(ps,param);
			// 查询操作
			rs = ps.executeQuery();
			rs.next();
			result = rs.getInt(1);

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			this.freeResultSet(rs);
			this.freeStatement(ps);
			this.freeConnection(conn);
		}

		return result;
	}

	/**
	 * 
	 * @param sql 例如：select * from tb where name=? and pwd=?
	 * @param args 和sql中的问号一一对应
	 * @return
	 */
	public List<Map<String, String>> queryForMap(String sql, List<Object> param) {

		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			// 设置sql 语句中的参数
			setParam(ps,param);
			rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int cols = rsmd.getColumnCount();
			while (rs.next()) {
				Map<String, String> map = new HashMap<String, String>();
				for (int j = 0; j < cols; j++) {
					String colName = rsmd.getColumnLabel(j + 1);
					int type = rsmd.getColumnType(j + 1);
					String val = parseResultSet(rs,rsmd,type,j);
					map.put(colName.toLowerCase(), val);
				}
				list.add(map);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.freeResultSet(rs);
			this.freeStatement(ps);
			this.freeConnection(conn);
		}
		
		return list;
	}

	/**
	 * 根据 sql 返回对象，sql 中查询的字段需要和bean中的一致，bean中属性必须为string类型
	 * @param sql eg:select id,name from tb_demo where name=? and pass =?
	 * @param param 参数列表，需要和sql 中的问号一一对应
	 * @param cls 保存数据库字段的javabean
	 * @return
	 */
	public <T> List<T> queryListObject(String sql, List<Object> param,Class<T> cls) {
		List<T> list = new ArrayList<T>();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			// 设置sql 语句中的参数
			setParam(ps,param);
			rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int cols = rsmd.getColumnCount();
			// 获取所有的方法
			Method[] methods = cls.getDeclaredMethods();
			Map<String, Method> meth = new HashMap<String, Method>();
			for (int k = 0; k < methods.length; k++) {
				// 获取方法名 转换成小写
				String methodName = methods[k].getName().toLowerCase();
				// 以set开头的方法 存入map中
				if (methodName.startsWith("set")) {
					meth.put(methodName, methods[k]);
				}
			}
			while (rs.next()) {
				// 构造bean对象，
				T obj = cls.newInstance();
				for (int j = 0; j < cols; j++) {
					String colName = "set" + rsmd.getColumnLabel(j + 1);
					Method m_tem = meth.get(colName.toLowerCase());
					if (m_tem != null) {
						int type = rsmd.getColumnType(j + 1);
						String val = parseResultSet(rs,rsmd,type,j);
						if (val == null) {
							val = "";
						}
						m_tem.invoke(obj, val);
					}
				}
				list.add(obj);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.freeResultSet(rs);
			this.freeStatement(ps);
			this.freeConnection(conn);
		}
		
		return list;
	}
	
	/**
	 * 根据 sql 返回单个对象，sql 中查询的字段需要和bean中的一致，不一致 需要使用 as 增加别名  id as userid
	 * @param sql eg:select id,name from tb_demo where name=? and pass =?
	 * @param param 参数列表，需要和sql 中的问号一一对应
	 * @param clazz 保存数据库字段的javabean
	 * @return
	 */
	public <T> T queryObject(String sql, List<Object> param,Class<T> clazz) {
		T obj = null;
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ResultSetMetaData rsmd = null;
		try {
			conn = getConnection();
			pstmt = conn.prepareStatement(sql);
			this.setParam(pstmt, param);
			rs = pstmt.executeQuery();
			rsmd = rs.getMetaData();
			int cols = rsmd.getColumnCount();
			Method[] methods = clazz.getDeclaredMethods();
			Map<String, Method> meth = new HashMap<String, Method>();
			for (int k = 0; k < methods.length; k++) {
				String methodName = methods[k].getName().toLowerCase();
				if (methodName.startsWith("set")) {
					meth.put(methodName, methods[k]);
				}
			}
			if(rs.next()) {
				obj = clazz.newInstance();
				for (int j = 0; j < cols; j++) {
					String colName = "set" + rsmd.getColumnLabel(j + 1);
					Method m_tem = meth.get(colName.toLowerCase());
					if (m_tem != null) {
						int type = rsmd.getColumnType(j + 1);
						String val = parseResultSet(rs,rsmd,type, j);
						if (val != null) {
							m_tem.invoke(obj, val);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.freeResultSet(rs);
			this.freeStatement(pstmt);
			this.freeConnection(conn);
		}
		
		return obj;
	}
}

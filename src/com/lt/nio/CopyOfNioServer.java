package com.lt.nio;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lt.utils.ConnectPoolC3P0;

public class CopyOfNioServer {
	
	static Logger logger = Logger.getLogger("WSLogger");
	static ConnectPoolC3P0 conn = ConnectPoolC3P0.getInstance();
	List<Object> param = new ArrayList<Object>();
	//通道管理器
    private Selector selector;
    private ServerSocketChannel serverChannel;
    private Map<String, Map<String,String>>  types;
    //获取一个ServerSocket通道，并初始化通道
    @SuppressWarnings("unchecked")
	public CopyOfNioServer init(String ip,int port) throws IOException{
        //获取一个ServerSocket通道
        serverChannel = ServerSocketChannel.open();
        /*非阻塞模式*/
        serverChannel.configureBlocking(false);
        
        //InetAddress ip = InetAddress.getLocalHost();
        serverChannel.socket().bind(new InetSocketAddress(ip,port));
        //获取通道管理器
        selector=Selector.open();
        //将通道管理器与通道绑定，并为该通道注册SelectionKey.OP_ACCEPT事件，
        //只有当该事件到达时，Selector.select()会返回，否则一直阻塞。
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        
        //类型对应
        /*String filePath = NioServer.class.getResource("/types.json").getPath();
        File json = new File(filePath);
        ObjectMapper mapper = new ObjectMapper();//此处引入的是jackson中的ObjectMapper类
        types = mapper.readValue(json, Map.class);*/
        return this;
    }

    /**
     * 停止
     * @throws Exception
     */
    public void Stop() throws Exception {
    	serverChannel.close();
        selector.close();
        logger.info("服务器端停止成功");
    }
    
    /**
     * 监听
     * @throws IOException
     */
    public void listen() throws IOException{
        System.out.println("服务器端启动成功");
        
        //使用轮询访问selector
        while(true){
            //当有注册的事件到达时，方法返回，否则阻塞。
            selector.select();
            //获取selector中的迭代器，选中项为注册的事件
            Iterator<SelectionKey> ite=selector.selectedKeys().iterator();
            
            while(ite.hasNext()){
                SelectionKey key = ite.next();
                //删除已选key，防止重复处理
                ite.remove();
                //客户端请求连接事件
                if(key.isAcceptable()){
                    ServerSocketChannel server = (ServerSocketChannel)key.channel();
                    //获得客户端连接通道
                    SocketChannel channel = server.accept();
                    channel.configureBlocking(false);
                    //向客户端发消息
                    //channel.write(ByteBuffer.wrap(new String("send message to client").getBytes()));
                    //在与客户端连接成功后，为客户端通道注册SelectionKey.OP_READ事件。
                    channel.register(selector, SelectionKey.OP_READ);
                    logger.info("客户端"+channel.getRemoteAddress()+"请求连接");
                }else if(key.isReadable()){//有可读数据事件
                    //获取客户端传输数据可读取消息通道。
                    SocketChannel channel = (SocketChannel)key.channel();
                    //创建读取数据缓冲器
                    ByteBuffer buffer = ByteBuffer.allocate(41);
                    //int read = channel.read(buffer);
                    channel.read(buffer);
                    byte[] data = buffer.array();
                    fomartDate(data,channel.getRemoteAddress()+"");
                    //printDate(data,channel.getRemoteAddress()+"");
                }
                else if (key.isWritable()) {// 写数据
                	logger.info(key.attachment()+ " - 写数据事件");
                    SocketChannel clientChannel = (SocketChannel) key.channel();
                    ByteBuffer sendBuf = ByteBuffer.allocate(10);
                    String sendText = "hello\n";
                    sendBuf.put(sendText.getBytes());
                    sendBuf.flip();        //写完数据后调用此方法
                    clientChannel.write(sendBuf);
                }
                else if (key.isConnectable()) {
                	logger.info(key.attachment()+ " - 连接事件");
                }
            }
        }
    }
    /**
     * 处理数据
     * */
    public void fomartDate(byte[] bytes,String address) {
        List<String> dataList=new ArrayList<String>();
        for(byte b : bytes) { 
            dataList.add(String.format("%02x", new Integer(b & 0xff)));
        }
        //传感器ID
        String deviceId=dataList.get(13)+dataList.get(14)+dataList.get(15)+dataList.get(16);
        
       //第二通道  
        if(Integer.parseInt(dataList.get(36), 16)==1){
        	param = new ArrayList<Object>();
            String sign=Integer.parseInt(dataList.get(37), 16)+"";
            //千百
            String data1=Integer.parseInt(dataList.get(38), 16)==0?"":Integer.parseInt(dataList.get(38), 16)+"";
            //十个
            String data2=Integer.parseInt(dataList.get(39), 16)==0?"":Integer.parseInt(dataList.get(39), 16)+"";
            //小数
            String data3=Integer.parseInt(dataList.get(40), 16)+"";
            if(sign.equals("1")){//负数
            	sign="-";
            }
            else{
            	sign="";
            }
            param.add("空气温度");
            param.add(sign+data1+data2+"."+data3);
        }
        //第一通道  
        if(Integer.parseInt(dataList.get(30), 16)==2){
            String sign=Integer.parseInt(dataList.get(31), 16)+"";
            //千百
            String data1=Integer.parseInt(dataList.get(32), 16)==0?"":Integer.parseInt(dataList.get(32), 16)+"";
            //十个
            String data2=Integer.parseInt(dataList.get(33), 16)==0?"":Integer.parseInt(dataList.get(33), 16)+"";
            //小数
            String data3=Integer.parseInt(dataList.get(34), 16)+"";
            if(sign.equals("1")){//负数
            	sign="-";
            }
            else{
            	sign="";
            }
            param.add("空气湿度");
            param.add(sign+data1+data2+"."+data3);
        }
        else if(Integer.parseInt(dataList.get(30), 16)==9){
            String sign=Integer.parseInt(dataList.get(31), 16)+"";
            //千百
            String data1=Integer.parseInt(dataList.get(32), 16)==0?"":Integer.parseInt(dataList.get(32), 16)+"";
            //十个
            String data2=Integer.parseInt(dataList.get(33), 16)==0?"":Integer.parseInt(dataList.get(33), 16)+"";
            //小数
            String data3=Integer.parseInt(dataList.get(34), 16)+"";
            if(sign.equals("1")){//负数
            	sign="-";
            }
            else{
            	sign="";
            }
            param.add("二氧化碳浓度");
            param.add(sign+data1+data2+"."+data3);
        }
        else if(Integer.parseInt(dataList.get(30), 16)==10){
            String sign=Integer.parseInt(dataList.get(31), 16)+"";
            //千百
            String data1=Integer.parseInt(dataList.get(32), 16)==0?"":Integer.parseInt(dataList.get(32), 16)+"";
            //十个
            String data2=Integer.parseInt(dataList.get(33), 16)==0?"":Integer.parseInt(dataList.get(33), 16)+"";
            //小数
            String data3=Integer.parseInt(dataList.get(34), 16)+"";
            if(sign.equals("1")){//负数
            	sign="-";
            }
            else{
            	sign="";
            }
            param.add("氨气浓度");
            param.add(sign+data1+data2+data3+"00");
             
        }
      
        if(param.size()==8){
        	insertForKey(address,deviceId,param);
        }
    }
    
    /**
     * 结果存数据库
     * @param address
     * @param param
     */
     
    private static void insertForKey(String address,String deviceId,List<Object> param){
    	address=address.replace("/", "");
    	String selectSql = "SELECT * FROM ENV_EQUIPMENT WHERE ENV_NUM ='"+deviceId+"' AND STATUS='启用'; ";
    	List<Map<String,String>> deviceList=conn.queryForMap(selectSql, null);
    	if(deviceList.size()>0){
    		 
    		String insertSql="INSERT INTO ENV_DATA " +
    				"(org_id,house_id,env_equipment_id,par_1,val_1,par_2,val_2,par_3,val_3,par_4,val_4) " +
    				"VALUES ("+deviceList.get(0).get("org_id")+","+deviceList.get(0).get("house")+","+deviceList.get(0).get("id")+",?,?,?,?,?,?,?,?);";
        	int result=conn.insertForKey(insertSql, param);
        	logger.info("插入数据，主键："+result);
    	}
    	else{
    		logger.info("未查询到设备("+deviceId+")，IP地址："+address);
    	}
    }
    /**
     * 输出数据
     * @param bytes
     * @param address
     */
    
    public void printDate(byte[] bytes,String address) {
        List<String> dataList=new ArrayList<String>();
        for(byte b : bytes) { 
            dataList.add(String.format("%02x", new Integer(b & 0xff)));
        }
        //System.out.println(types.get("1").get("name"));
        ObjectMapper mapper = new ObjectMapper();
        
        Map<String,String> resultMap=new HashMap<String,String>();
        resultMap.put("客户端地址", address);
        resultMap.put("传感器ID", dataList.get(13)+dataList.get(14)+dataList.get(15)+dataList.get(16));
        resultMap.put("发送周期", dataList.get(18)+"分钟");
        resultMap.put("信号强度", dataList.get(20));
        resultMap.put("发送时间", "20"+dataList.get(21)+"-"+dataList.get(22)+"-"+dataList.get(23)+" "+
        		dataList.get(24)+":"+dataList.get(25)+":"+dataList.get(26));
        resultMap.put("校验和", Integer.parseInt(dataList.get(27),16)+""); 
        resultMap.put("通道数", dataList.get(28)); 
        resultMap.put("通道编号", dataList.get(29)); 
         
        //类型
        String typeCode=Integer.parseInt(dataList.get(30), 16)+"";
        String sign=Integer.parseInt(dataList.get(31), 16)+"";
        //千百
        String data1=Integer.parseInt(dataList.get(32), 16)==0?"":Integer.parseInt(dataList.get(32), 16)+"";
        //十个
        String data2=Integer.parseInt(dataList.get(33), 16)==0?"":Integer.parseInt(dataList.get(33), 16)+"";
        //小数
        String data3=Integer.parseInt(dataList.get(34), 16)+"";
        if(sign.equals("1")){//负数
        	sign="-";
        }
        else{
        	sign="";
        }
        
        if(typeCode.equals("3")){
        	resultMap.put(types.get(typeCode).get("name"),sign+data1+data2+data3+types.get(typeCode).get("unit"));
        }
        else if(typeCode.equals("10")){
        	resultMap.put(types.get(typeCode).get("name"),sign+data1+data2+data3+"00"+types.get(typeCode).get("unit"));
        }
        else{
        	
        	resultMap.put(types.get(typeCode).get("name"),sign+data1+data2+"."+data3+types.get(typeCode).get("unit"));
        }
        //logger.info("收到数据, 大小:" + bytes.length + " 数据: " + StringUtils.join(dataList," "));
        try {
			String mapJakcson = mapper.writeValueAsString(resultMap);
			logger.info("解析后数据："+mapJakcson);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        

       
        //第二通道   type和channel 
        /*if(Integer.parseInt(dataList.get(36), 16)!=0){
        	 Map<String,String> resultMap2=new HashMap<String,String>();
        	 resultMap2.put("客户端地址", address);
        	 resultMap2.put("传感器ID", dataList.get(13)+dataList.get(14)+dataList.get(15)+dataList.get(16));
        	 resultMap2.put("发送周期", dataList.get(18)+"分钟");
        	 resultMap2.put("信号强度", dataList.get(20));
        	 resultMap2.put("发送时间", "20"+dataList.get(21)+"-"+dataList.get(22)+"-"+dataList.get(23)+" "+
             		dataList.get(24)+":"+dataList.get(25)+":"+dataList.get(26));
        	 resultMap2.put("校验和", Integer.parseInt(dataList.get(27),16)+""); 
        	 resultMap2.put("通道数", dataList.get(28)); 
        	 resultMap2.put("通道编号", dataList.get(29));
        	 
        	//类型
             String typeCode2=Integer.parseInt(dataList.get(36), 16)+"";
             String sign2=Integer.parseInt(dataList.get(37), 16)+"";
             //千百
             String data21=Integer.parseInt(dataList.get(38), 16)==0?"":Integer.parseInt(dataList.get(38), 16)+"";
             //十个
             String data22=Integer.parseInt(dataList.get(39), 16)==0?"":Integer.parseInt(dataList.get(39), 16)+"";
             //小数
             String data23=Integer.parseInt(dataList.get(40), 16)+"";
             if(sign2.equals("1")){//负数
            	 sign2="-";
             }
             else{
            	 sign2="";
             }
             
             if(typeCode2.equals("3")){
             	resultMap2.put(types.get(typeCode2).get("name"),sign2+data21+data22+data23+types.get(typeCode2).get("unit"));
             }
             else if(typeCode2.equals("10")){
             	resultMap2.put(types.get(typeCode2).get("name"),sign2+data21+data22+data23+"00"+types.get(typeCode2).get("unit"));
             }
             else{
             	
             	resultMap2.put(types.get(typeCode2).get("name"),sign2+data21+data22+"."+data23+types.get(typeCode2).get("unit"));
             }
             try {
     			String mapJakcson = mapper.writeValueAsString(resultMap2);
     			logger.info("解析后数据："+mapJakcson);
     		} catch (JsonProcessingException e) {
     			// TODO Auto-generated catch block
     			e.printStackTrace();
     		}
        }*/
        
        
    }
    

    
    
    
    
    public static void main(String[] args) throws IOException {
    	
    	Properties properties = new Properties();
    	 
    	String path = System.getProperty("user.dir") + "/src/ip/ip.properties";
    	properties.load(new FileInputStream(path));
    	String ip=properties.getProperty("ip").trim();
    	int port = Integer.parseInt(properties.getProperty("port").trim());
        new CopyOfNioServer().init(ip,port).listen();
        
    	//insertForKey("001",null);
        /*String sign=Integer.parseInt("01", 16)+"";
        System.out.println(sign);*/
    	 
    }
  
    
}

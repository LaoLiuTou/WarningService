package com.lt.nio;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
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

public class YsNioServer {
	
	static Logger logger = Logger.getLogger("WSLogger");
	static ConnectPoolC3P0 conn = ConnectPoolC3P0.getInstance();
	List<Object> param = new ArrayList<Object>();
	//通道管理器
    private Selector selector;
    private ServerSocketChannel serverChannel;
    private Map<String, Map<String,String>>  types;
    //获取一个ServerSocket通道，并初始化通道
    @SuppressWarnings("unchecked")
	public YsNioServer init(String ip,int port){
        try {
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
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.info(e);
		} 
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
                	ByteBuffer buffer = ByteBuffer.allocate(65535);
                	byte[] chTemp = new byte[4096];
                	int iRecvLen = 0;
                    channel.read(buffer); 
					buffer.flip();
					iRecvLen = buffer.limit(); 
					if(iRecvLen>0){
						Bcd2AscEx(chTemp, buffer.array(), iRecvLen* 2); 
						String s = new String(chTemp, "UTF-8");
						System.out.println("receive message[" + iRecvLen + "]: "
								+ s.trim());
					}
					

					buffer.clear();
                    
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
    public static void Bcd2AscEx(byte asc[], byte bcd[], int len)
    {
        int	i, j;
        int k;

        j = (len + len%2) / 2;
        k = 3*j;
        for(i=0; i<j; i++)
        {
                asc[3*i]	= (byte)((bcd[i] >> 4) & 0x0f);
                asc[3*i+1]	= (byte)(bcd[i] & 0x0f);
                asc[3*i+2]	= 0x20;
        }
        for(i=0; i<k; i++)
        {
                if ( (i+1) % 3 == 0 )
                {
                        continue;
                }
                if( asc[i] > 0x09)
                {
                        asc[i]	= (byte)(0x41 + asc[i] - 0x0a);
                }
                else	
                {
                        asc[i]	+= 0x30;
                }
        }

        asc[k] = 0;
        
    }
    
    public static void main(String[] args) throws IOException {
    	InetAddress address = InetAddress.getLocalHost();//获取的是本地的IP地址 //PC-20140317PXKX/192.168.0.121
    	Properties properties = new Properties();
    	 
    	String path = System.getProperty("user.dir") + "/src/ip/ip.properties";
    	properties.load(new FileInputStream(path));
    	//String ip=properties.getProperty("ip").trim();
    	String ip=address.getHostAddress();
    	int port = Integer.parseInt(properties.getProperty("port").trim());
        new YsNioServer().init(ip,port).listen();
        
    	//insertForKey("001",null);
        /*String sign=Integer.parseInt("01", 16)+"";
        System.out.println(sign);*/
    	 
    }
  
    
}

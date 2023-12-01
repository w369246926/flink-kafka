import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

public class HttpsServer {

    public static void main(String[] args) {
        try {
            // 创建一个HttpsServer对象，绑定端口号为8443
            HttpsServer server = HttpsServer.create(new InetSocketAddress(8443), 0);

            // 创建一个SSLContext对象，用于配置SSL相关参数
            SSLContext sslContext = SSLContext.getInstance("TLS");

            // 从文件中加载服务端的密钥库和信任库
            char[] password = "password".toCharArray();
            java.security.KeyStore ks = java.security.KeyStore.getInstance("JKS");
            ks.load(HttpsServer.class.getResourceAsStream("server.jks"), password);
            java.security.KeyStore ts = java.security.KeyStore.getInstance("JKS");
            ts.load(HttpsServer.class.getResourceAsStream("servertrust.jks"), password);

            // 创建一个KeyManagerFactory对象，用于管理服务端的密钥
            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, password);

            // 创建一个TrustManagerFactory对象，用于管理服务端的信任
            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(ts);

            // 初始化SSLContext对象，设置密钥管理器和信任管理器
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

            // 设置HttpsServer的配置器，使用SSLContext对象
            server.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
                public void configure(HttpsParameters params) {
                    try {
                        // 获取SSLContext对象
                        SSLContext context = getSSLContext();

                        // 获取SSL参数对象
                        SSLParameters sslparams = context.getDefaultSSLParameters();

                        // 设置SSL参数对象
                        params.setSSLParameters(sslparams);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            // 创建一个HttpHandler对象，用于处理客户端的请求和响应
            HttpHandler handler = new HttpHandler() {
                public void handle(HttpExchange exchange) {
                    try {
                        // 获取请求方法
                        String method = exchange.getRequestMethod();
                        System.out.println("Request Method: " + method);

                        // 获取请求数据
                        BufferedReader in = new BufferedReader(new InputStreamReader(exchange.getRequestBody()));
                        String line;
                        StringBuilder request = new StringBuilder();
                        while ((line = in.readLine()) != null) {
                            request.append(line);
                        }
                        in.close();

                        // 打印请求数据
                        System.out.println("Request Data: " + request.toString());

                        // 设置响应码
                        int responseCode = 200;
                        exchange.sendResponseHeaders(responseCode, 0);

                        // 设置响应数据
                        String data = "{\"name\":\"Bob\",\"message\":\"Hi\"}";

                        // 获取输出流，发送响应数据
                        OutputStream out = exchange.getResponseBody();
                        out.write(data.getBytes());
                        out.flush();
                        out.close();

                        // 打印响应码
                        System.out.println("Response Code: " + responseCode);

                        // 打印响应数据
                        System.out.println("Response Data: " + data);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            // 为HttpsServer添加一个上下文，指定请求路径和处理器
            server.createContext("/", handler);

            // 设置HttpsServer的线程池
            server.setExecutor(Executors.newCachedThreadPool());

            // 启动HttpsServer
            server.start();

            System.out.println("HttpsServer started at port 8443");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

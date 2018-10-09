package cst.jstorm.hour.bolt.gps;

import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;

/**
 * @author Johnney.Chiu
 * create on 2018/2/1 16:05
 * @Description
 * @title
 */
public class Test3 {
    private AbstractApplicationContext beanContext;
    private Logger logger;

    public void init(){
        beanContext = MyApplicationContext.getDefaultContext();
        logger = LoggerFactory.getLogger(Test3.class);
    }
    public static void main(String... args)  {
        Test3 test3 = new Test3();
        Thread threads[]=new Thread[30];
        for(int i=29;i>=0;i--)
            threads[i]=new Thread(new Runnable() {
                @Override
                public void run() {
                    test3.init();
                }
            },"thread ["+i+"]");
        for (Thread thread: threads) {
            thread.start();
        }
    }
}

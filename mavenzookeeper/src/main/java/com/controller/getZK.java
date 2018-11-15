package com.controller;

import com.utils.ZkClientService;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/zkController")
public class getZK {
    @Autowired
    private ZkClientService zkClient;
    @RequestMapping("/getZK")
    @ResponseBody
    public String getZK(){
        try {
            List<String> children = zkClient.getChildren("/");
            System.out.println(children.stream().toString());
            //创建分布式锁
            //InterProcessLock interProcessLock = zkClient.getInterProcessLock("/");
            //分布式锁测试
            for(int i=0;i<50;i++){
                new Thread(()->{
                    InterProcessLock lock = null;
                    try{
                        lock = zkClient.getInterProcessLock("/distributeLock");
                        System.out.println(Thread.currentThread().getName()+"申请锁");
                        lock.acquire();
                        System.out.println(Thread.currentThread().getName()+"持有锁");
                        Thread.sleep(500);
                    }
                    catch(Exception e){
                        e.printStackTrace();
                    }
                    finally{
                        if(null != lock){
                            try {
                                lock.release();
                                System.out.println(Thread.currentThread().getName()+"释放有锁");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }).start();
            }
            //监听节点测试
            zkClient.watchNode("/",(client,event)->{
                String data=new String(event.getData().getData());
                switch (event.getType()){
                    case CHILD_ADDED: {
                        System.out.println("Node added: " + data);
                        break;
                    }
                    case CHILD_UPDATED: {
                        System.out.println("Node changed: " + data);
                        break;
                    }
                    case CHILD_REMOVED: {
                        System.out.println("Node removed: " + data);
                        break;
                    }

                }
            });
            return "ok";
        }catch (Exception e){
            e.printStackTrace();
            return "error";
        }

    }
}

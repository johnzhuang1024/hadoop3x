package com.atguigu.rpc;

/**
 * @author JohnZhuang
 * @version 1.0
 * @description: TODO
 * @date 2023/2/25 10:18
 */
public interface RPCProtocol {
    long versionID = 666;

    void mkdirs(String path);
}

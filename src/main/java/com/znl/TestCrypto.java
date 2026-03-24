package com.znl;

public class TestCrypto {
    public static void main(String[] args) throws Exception {
        SecurityUtils.Keys keys = SecurityUtils.deriveKeys("my-secret");
        System.out.println("signKey hex: " + SecurityUtils.bytesToHex(keys.signKey));
        System.out.println("encryptKey hex: " + SecurityUtils.bytesToHex(keys.encryptKey));
    }
}

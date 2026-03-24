package com.znl;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

public class TestCryptoEnc {
    public static void main(String[] args) throws Exception {
        SecurityUtils.Keys keys = SecurityUtils.deriveKeys("my-secret");
        byte[] iv = new byte[]{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, (byte)0x88, (byte)0x99, (byte)0xaa, (byte)0xbb};
        byte[] plaintext = "hello world".getBytes(StandardCharsets.UTF_8);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec spec = new GCMParameterSpec(16 * 8, iv);
        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(keys.encryptKey, "AES"), spec);
        
        byte[] ciphertextAndTag = cipher.doFinal(plaintext);
        int cipherLen = ciphertextAndTag.length - 16;
        byte[] ciphertext = new byte[cipherLen];
        byte[] tag = new byte[16];
        System.arraycopy(ciphertextAndTag, 0, ciphertext, 0, cipherLen);
        System.arraycopy(ciphertextAndTag, cipherLen, tag, 0, 16);

        System.out.println("ciphertext: " + SecurityUtils.bytesToHex(ciphertext));
        System.out.println("tag: " + SecurityUtils.bytesToHex(tag));
    }
}

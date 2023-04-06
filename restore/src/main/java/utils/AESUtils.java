package utils;

import org.apache.commons.codec.binary.Base64;

import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * AES加密工具类
 * MD5值后16位为key,AES采用128位ECB模式,填充模式pkcs5,加密之后再base64转码
 */
public class AESUtils {


    /**
     * 加密
     *
     * @param content
     * @param key
     * @return
     * @throws Exception
     */
    public static String Encrypt(String content, String key) {
        if (key == null) {
            System.out.print("Key为空null");
            return null;
        }
        // 判断Key是否为16位
        if (key.length() != 16) {
            System.out.print("Key长度不是16位");
            return null;
        }
        KeyGenerator keyGenerator = null;
        try {
            keyGenerator = KeyGenerator.getInstance("AES");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        keyGenerator.init(128, new SecureRandom(key.getBytes()));
        SecretKey secretKey = keyGenerator.generateKey();
        byte[] enCodeFormat = secretKey.getEncoded();
        SecretKeySpec keySpec = new SecretKeySpec(enCodeFormat, "AES");
        //"算法/模式/补码方式"
        Cipher cipher = null;
        try {
            cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
        }
        try {
            cipher.init(Cipher.ENCRYPT_MODE, keySpec);
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        }
        byte[] encrypted = new byte[0];
        try {
            encrypted = cipher.doFinal(content.getBytes("utf-8"));
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        } catch (BadPaddingException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        //使用BASE64做转码功能，能起到2次加密的作用。
        return new Base64().encodeToString(encrypted);
    }

    /**
     * 解密
     *
     * @param content
     * @param key
     * @return
     * @throws Exception
     */
    public static String Decrypt(String content, String key) {
        try {
            // 判断Key是否为16位
            if (key.length() != 16) {
                System.out.print("Key长度不是16位");
                return null;
            }
            KeyGenerator kgen = KeyGenerator.getInstance("AES");
            kgen.init(128, new SecureRandom(key.getBytes()));
            SecretKey secretKey = kgen.generateKey();
            byte[] enCodeFormat = secretKey.getEncoded();
            SecretKeySpec keySpec = new SecretKeySpec(enCodeFormat, "AES");
            //"算法/模式/补码方式"
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, keySpec);
            //先base64解密
            byte[] encrypted1 = new Base64().decode(content);
            try {
                byte[] original = cipher.doFinal(encrypted1);
                String originalString = new String(original, "utf-8");
                return originalString;
            } catch (Exception e) {
                System.out.println(e.toString());
                return null;
            }
        } catch (Exception ex) {
            System.err.println(ex.toString());
            return null;
        }
    }


    public static void main(String[] args) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        //此处使用AES-128-ECB加密模式，key需要为16位。
        String key = "8749284927849211";
        // 原字符串
        String content = "hello world！";
        System.out.println(simpleDateFormat.format(new Date()) + "原字符串是：" + content);
        // 加密
        String encryptContent = null;

        encryptContent = AESUtils.Encrypt(content, key);

        System.out.println(simpleDateFormat.format(new Date()) + "加密后的字符串是：" + encryptContent);
        // 解密
        String decryptContent = AESUtils.Decrypt(encryptContent, key);
        System.out.println(simpleDateFormat.format(new Date()) + "解密后的字符串是：" + decryptContent);
    }


}


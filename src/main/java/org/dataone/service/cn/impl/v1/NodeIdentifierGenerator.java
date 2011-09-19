/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.service.cn.impl.v1;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author waltz
 */
public class NodeIdentifierGenerator {
    static private SecureRandom random;

    public static Log log = LogFactory.getLog(NodeIdentifierGenerator.class);
    static {
        // Need this or context will lowercase all the rdn s

        // randomly generate new Ids for register function
        try {
            random = SecureRandom.getInstance("SHA1PRNG");
        } catch (NoSuchAlgorithmException ex) {
            random = null;
            log.error(ex.getMessage());
        }
    }
    static private String alphabet = "abcdefghijkmnopqrstuvwxyzABCDEFGHIJKLMNPQRSTUVWXYZ";
    static private String numbers = "23456789";

    static public String generateId(int randomGenArrayLength) {
        if (randomGenArrayLength == 4) {
            char randString[] = new char[4];
            randString[0] = alphabet.charAt(random.nextInt(alphabet.length()));
            randString[1] = numbers.charAt(random.nextInt(numbers.length()));
            randString[2] = alphabet.charAt(random.nextInt(alphabet.length()));
            randString[3] = numbers.charAt(random.nextInt(numbers.length()));
            return new String(randString);
        } else {
            char randString[] = new char[6];
            randString[0] = alphabet.charAt(random.nextInt(alphabet.length()));
            randString[1] = numbers.charAt(random.nextInt(numbers.length()));
            randString[2] = alphabet.charAt(random.nextInt(alphabet.length()));
            randString[3] = numbers.charAt(random.nextInt(numbers.length()));
            randString[4] = alphabet.charAt(random.nextInt(alphabet.length()));
            randString[5] = numbers.charAt(random.nextInt(numbers.length()));
            return new String(randString);
        }
    }
}

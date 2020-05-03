import static java.io.File.separator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Random;
import java.util.Scanner;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

/**
 * TODO Put here a description of what this class does.
 *
 * @author Purnendu.
 *         Created May 3, 2020.
 *         Folder Structure Root Dir -- temp
 *         Plaintext in rootDir
 *         Enc text in temp folder inside rootDir
 *         ------------------------------------
 *         Command line arguments--
 *         1. rootDir path
 *         2. File name of file to be encrypted
 *         3. password
 */
public class Enc {
	public static void main(final String[] args) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException,
			InvalidAlgorithmParameterException, NoSuchPaddingException, IOException, IllegalBlockSizeException, BadPaddingException {
		final Scanner sc = new Scanner(System.in);
		final String rootDir = sc.next();
		final String tempDir = rootDir + separator + "temp";
		new File(tempDir).mkdirs();
		final String fileName = sc.next();

		final FileInputStream inFile = new FileInputStream(rootDir + separator + fileName);
		final FileOutputStream outFile = new FileOutputStream(tempDir + separator + fileName);
		final String password = sc.next();
		final PBEKeySpec pbeKeySpec = new PBEKeySpec(password.toCharArray());
		final SecretKeyFactory secretKeyFactory = SecretKeyFactory
				.getInstance("PBEWithMD5AndTripleDES");
		final SecretKey secretKey = secretKeyFactory.generateSecret(pbeKeySpec);

		final byte[] salt = new byte[8];
		final Random random = new Random();
		random.nextBytes(salt);

		final PBEParameterSpec pbeParameterSpec = new PBEParameterSpec(salt, 100);
		final Cipher cipher = Cipher.getInstance("PBEWithMD5AndTripleDES");
		cipher.init(Cipher.ENCRYPT_MODE, secretKey, pbeParameterSpec);
		outFile.write(salt);

		final byte[] input = new byte[64];
		int bytesRead;
		while ((bytesRead = inFile.read(input)) != -1) {
			final byte[] output = cipher.update(input, 0, bytesRead);
			if (output != null) {
				outFile.write(output);
			}
		}

		final byte[] output = cipher.doFinal();
		if (output != null) {
			outFile.write(output);
		}

		inFile.close();
		outFile.flush();
		outFile.close();
	}
}

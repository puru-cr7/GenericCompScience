import static java.io.File.separator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
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
 *         Enc text in temp folder inside rootDir
 *         Decrypted text will be generated inside temp folder with same file name appended by decrypted
 *         ------------------------------------
 *         Command line arguments--
 *         1. rootDir path
 *         2. File name of file to be decrypted
 *         3. password
 */
public class Dec {
	public static void main(final String[] args)
			throws NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException, IOException, InvalidKeyException,
			InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException {
		final Scanner sc = new Scanner(System.in);
		final String rootDir = sc.next();
		final String tempDir = rootDir + separator + "temp";
		new File(tempDir).mkdirs();
		final String fileName = sc.next();
		final String password = sc.next();
		final PBEKeySpec pbeKeySpec = new PBEKeySpec(password.toCharArray());
		final SecretKeyFactory secretKeyFactory = SecretKeyFactory
				.getInstance("PBEWithMD5AndTripleDES");
		final SecretKey secretKey = secretKeyFactory.generateSecret(pbeKeySpec);

		final FileInputStream fis = new FileInputStream(tempDir + separator + fileName);
		final byte[] salt = new byte[8];
		fis.read(salt);

		final PBEParameterSpec pbeParameterSpec = new PBEParameterSpec(salt, 100);

		final Cipher cipher = Cipher.getInstance("PBEWithMD5AndTripleDES");
		cipher.init(Cipher.DECRYPT_MODE, secretKey, pbeParameterSpec);
		final FileOutputStream fos = new FileOutputStream(tempDir + separator + fileName + "_decrypted_");
		final byte[] in = new byte[64];
		int read;
		while ((read = fis.read(in)) != -1) {
			final byte[] output = cipher.update(in, 0, read);
			if (output != null) {
				fos.write(output);
			}
		}

		final byte[] output = cipher.doFinal();
		if (output != null) {
			fos.write(output);
		}

		fis.close();
		fos.flush();
		fos.close();
	}
}


public class Driver {
	public static void main(String[] args) throws Exception {
		DataDividedByUser dataDividedByUser = new DataDividedByUser();
		GenerateCoOccurrence unNormalizedCoOccurrenceMatrix = new GenerateCoOccurrence();
		Normalize normailzedCoOccurrenceMatrix = new Normalize();
		Multiplication multiplication = new Multiplication();
		Sum sum = new Sum();
		ToDataBase toDataBase = new ToDataBase();

		String rawInput = args[0];
		String dataDividedByUserOutput = args[1];
		String unNormalizedCoOccurrenceMatrixOutput = args[2];
		String normalizedCoOccurrenceMatrixOutput = args[3];
		String multiplicationOutput = args[4];
		String sumOutput = args[5];
		String toDataBaseInput = sumOutput;

		String[] pathsForDataDividedByUser = {rawInput, dataDividedByUserOutput};
		String[] pathsForGenerateCoOccurrence = {dataDividedByUserOutput, unNormalizedCoOccurrenceMatrixOutput};
		String[] pathsForNomalize = {unNormalizedCoOccurrenceMatrixOutput, normalizedCoOccurrenceMatrixOutput};
		String[] pathsForMutiplication = {normalizedCoOccurrenceMatrixOutput, rawInput, multiplicationOutput};
		String[] pathsForSum = {multiplicationOutput, sumOutput};
		String[] pathsForToDataBase = {toDataBaseInput};

		DataDividedByUser.main(pathsForDataDividedByUser);
		GenerateCoOccurrence.main(pathsForGenerateCoOccurrence);
		Normalize.main(pathsForNomalize);
		Multiplication.main(pathsForMutiplication);
		Sum.main(pathsForSum);
		ToDataBase.main(pathsForToDataBase);
	}
}

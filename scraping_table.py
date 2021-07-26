import tabula
import camelot

file = "C:\\users\\denicolaf\\downloads\\Excess funds all years.pdf"

# table = tabula.read_pdf(file,pages=1)
table = camelot.read_pdf(file)
print(table[0].df)

ind = table[0].df.drop(0,axis=0)
ind = ind.drop(1,axis=0)

ind = ind.drop(0,axis=1)

ind = ind.reset_index(drop=True)

ind = ind.rename(columns=ind.iloc[0]).drop(ind.index[0])

tmp = ind
print(tmp)

import re


# import sys

def urdu_to_english(text):
    return text.translate(
        {ord(u'\u035c'): u'i'  # arc below between Allah and laam
            , ord(u'\u060c'): u','  # comma
            , ord(u'\u0610'): u''  # something after Nabi
            , ord(u'\u0611'): u''  # something above noon in Hussain
            , ord(u'\u0612'): u''  # something after hazrat Sultan Bahu
            , ord(u'\u0614'): u'o'  # wide pesh  Ghalibo?
            , ord(u'\u061b'): u';'  # semi colon
            , ord(u'\u061f'): u'?'  # question mark
            , ord(u'\u0621'): u''  # hamza
            , ord(u'\u0622'): u'aa'  # alif madda
            , ord(u'\u0623'): u'a'  # alif high hamza
            , ord(u'\u0624'): u'o'  # wow hamza above
            , ord(u'\u0625'): u'i'  # alif low hamza
            , ord(u'\u0626'): u'i'  # yay hamza
            , ord(u'\u0627'): u'a'  # alif
            , ord(u'\u0628'): u'b'  # bay
            , ord(u'\u0629'): u'a'  # ha with w above
            , ord(u'\u062a'): u't'  # tay
            , ord(u'\u062b'): u'th'  # say
            , ord(u'\u062c'): u'j'  # jeem
            , ord(u'\u062d'): u'h'  # hay
            , ord(u'\u062e'): u'KH'  # khay
            , ord(u'\u062f'): u'd'  # daal
            , ord(u'\u0630'): u'z'  # zaal
            , ord(u'\u0631'): u'r'  # ray
            , ord(u'\u0632'): u'z'  # zay
            , ord(u'\u0633'): u's'  # seen
            , ord(u'\u0634'): u'SH'  # sheen
            , ord(u'\u0635'): u's'  # suad
            , ord(u'\u0636'): u'z'  # zuad
            , ord(u'\u0637'): u't'  # toay
            , ord(u'\u0638'): u'z'  # zoay
            , ord(u'\u0639'): u'a'  # ain
            , ord(u'\u063a'): u'GH'  # ghain
            , ord(u'\u0640'): u''  # big dot
            , ord(u'\u0641'): u'f'  # fay
            , ord(u'\u0642'): u'q'  # qaaf
            , ord(u'\u0643'): u'k'  # kaaf
            , ord(u'\u0644'): u'l'  # laam
            , ord(u'\u0645'): u'm'  # meem
            , ord(u'\u0646'): u'n'  # noon
            , ord(u'\u0647'): u'h'  # ha
            , ord(u'\u0648'): u'w'  # wow
            , ord(u'\u0649'): u'y'  # yay
            , ord(u'\u064a'): u'y'  # choti yay with 2 dots below
            , ord(u'\u064b'): u'an'  # double zabar
            , ord(u'\u064c'): u'u'  # pesh
            , ord(u'\u064d'): u'in'  # double zer
            , ord(u'\u064e'): u'a'  # zabar
            , ord(u'\u064f'): u'u'  # pesh
            , ord(u'\u0650'): u'i'  # zer
            , ord(u'\u0651'): u''  # doubler (w) above
            , ord(u'\u0652'): u''  # circle above
            , ord(u'\u0653'): u'a'  # madda
            , ord(u'\u0654'): u''  # hamza above
            , ord(u'\u0657'): u''  # something above yay in gaey
            , ord(u'\u0660'): u'0'  # digit
            , ord(u'\u0661'): u'1'  # digit
            , ord(u'\u0662'): u'2'  # digit
            , ord(u'\u0663'): u'3'  # digit
            , ord(u'\u0664'): u'4'  # digit
            , ord(u'\u0665'): u'5'  # digit
            , ord(u'\u0666'): u'6'  # digit
            , ord(u'\u0667'): u'7'  # digit
            , ord(u'\u0668'): u'8'  # digit
            , ord(u'\u0669'): u'9'  # digit
            , ord(u'\u066d'): u'*'  # asterix
            , ord(u'\u0670'): u'a'  # khari zabar
            , ord(u'\u0671'): u'a'  # alif with something above
            , ord(u'\u0674'): u''  # hamza
            , ord(u'\u0679'): u'T'  # Tay
            , ord(u'\u067c'): u't'  # tay with something below
            , ord(u'\u067d'): u's'  # say
            , ord(u'\u067e'): u'p'  # pay
            , ord(u'\u0681'): u'KH'  # khay
            , ord(u'\u0685'): u'sh'  # chay with 3 dots moved above
            , ord(u'\u0686'): u'CH'  # chay
            , ord(u'\u0688'): u'd'  # Daal
            , ord(u'\u0689'): u'd'  # daal with something below
            , ord(u'\u0691'): u'r'  # Ray
            , ord(u'\u0693'): u'r'  # ray with something below
            , ord(u'\u0698'): u's'  # Say
            , ord(u'\u069a'): u'bn'  # bay noon noon-ghunna
            , ord(u'\u06a9'): u'k'  # kaaf
            , ord(u'\u06ab'): u'k'  # kaaf with something below arm
            , ord(u'\u06af'): u'g'  # gaaf
            , ord(u'\u06b3'): u'g'  # gaaf with something below
            , ord(u'\u06ba'): u'n'  # noon ghunna
            , ord(u'\u06be'): u'h'  # ha double
            , ord(u'\u06c1'): u'h'  # ha
            , ord(u'\u06c2'): u'ah'  # ha with something above
            , ord(u'\u06c3'): u'ah'  # ha with 2 dots above
            , ord(u'\u06c6'): u'wa'  # wow with something above
            , ord(u'\u06cc'): u'y'  # yay
            , ord(u'\u06cd'): u'ya'  # choti yay alif
            , ord(u'\u06d0'): u'i'  # choti yay with something below
            , ord(u'\u06d2'): u'ay'  # bari yay
            , ord(u'\u06d3'): u'ay'  # bari yay something above
            , ord(u'\u06d4'): u'-'  # dash
            , ord(u'\u06d5'): u'h'  # ha
            , ord(u'\u06d6'): u''  # tiny suad laam bari yay in arabic
            , ord(u'\u06d7'): u''  # tiny something in arabic
            , ord(u'\u06d9'): u''  # tiny laam alif in arabic
            , ord(u'\u06da'): u''  # tiny jeem in arabic
            , ord(u'\u06dd'): u'.'  # big circle divider
            , ord(u'\u06de'): u'.'  # star divider
            , ord(u'\u06e1'): u''  # symbol on top of laam in qul
            , ord(u'\u06e9'): u'(H)'  # house symbol
            , ord(u'\u06f0'): u'0'  # digit
            , ord(u'\u06f1'): u'1'  # digit
            , ord(u'\u06f2'): u'2'  # digit
            , ord(u'\u06f3'): u'3'  # digit
            , ord(u'\u06f4'): u'4'  # digit
            , ord(u'\u06f5'): u'5'  # digit
            , ord(u'\u06f6'): u'6'  # digit
            , ord(u'\u06f7'): u'7'  # digit
            , ord(u'\u06f8'): u'8'  # digit
            , ord(u'\u06f9'): u'9'  # digit
            , ord(u'\ufb50'): u'a'  # alif with something above
            , ord(u'\ufb51'): u'l'  # laam- with something above
            , ord(u'\ufb56'): u'p'  # pay
            , ord(u'\ufb57'): u'p'  # -pay
            , ord(u'\ufb58'): u'p'  # pay-
            , ord(u'\ufb59'): u'p'  # -pay-
            , ord(u'\ufb66'): u'T'  # Tay
            , ord(u'\ufb67'): u'T'  # -Tay
            , ord(u'\ufb68'): u'T'  # Tay-
            , ord(u'\ufb69'): u'T'  # -Tay-
            , ord(u'\ufb7a'): u'CH'  # chay
            , ord(u'\ufb7b'): u'CH'  # -chay
            , ord(u'\ufb7c'): u'CH'  # chay-
            , ord(u'\ufb7d'): u'CH'  # -chay-
            , ord(u'\ufb88'): u'd'  # Daal
            , ord(u'\ufb89'): u'd'  # -Daal
            , ord(u'\ufb8c'): u'r'  # Ray
            , ord(u'\ufb8d'): u'r'  # -Ray
            , ord(u'\ufb8e'): u'k'  # kaaf
            , ord(u'\ufb8f'): u'k'  # -kaaf
            , ord(u'\ufb90'): u'k'  # kaaf-
            , ord(u'\ufb91'): u'k'  # -kaaf-
            , ord(u'\ufb92'): u'g'  # gaaf
            , ord(u'\ufb93'): u'g'  # -gaaf
            , ord(u'\ufb94'): u'g'  # gaaf-
            , ord(u'\ufb95'): u'g'  # -gaaf-
            , ord(u'\ufb9e'): u'n'  # noon ghunna
            , ord(u'\ufb9f'): u'n'  # -noon ghunna
            , ord(u'\ufba6'): u'h'  # ha
            , ord(u'\ufba7'): u'ah'  # -ha
            , ord(u'\ufba8'): u'h'  # ha-
            , ord(u'\ufba9'): u'ah'  # -ha-
            , ord(u'\ufbaa'): u'h'  # ha double
            , ord(u'\ufbab'): u'h'  # -ha double
            , ord(u'\ufbac'): u'h'  # ha double-
            , ord(u'\ufbad'): u'h'  # -ha double-
            , ord(u'\ufbae'): u'ay'  # bari yay
            , ord(u'\ufbaf'): u'ay'  # -bari yay
            , ord(u'\ufbb0'): u'aey'  # -bari yay something above
            , ord(u'\ufbb1'): u'aey'  # -bari yay hamza above
            , ord(u'\ufbfc'): u'i'  # yay
            , ord(u'\ufbfd'): u'i'  # -yay
            , ord(u'\ufbfe'): u'y'  # yay-
            , ord(u'\ufbff'): u'y'  # -yay-
            , ord(u'\ufd3e'): u')'  # fancy bracket open
            , ord(u'\ufd3f'): u'('  # fancy bracket close
            , ord(u'\ufdf2'): u'Allah'  # Allah
            , ord(u'\ufdfa'): u'(SAW)'  # sallallahu alaihi wasallam
            , ord(u'\ufdfd'): u'(Bismillah)'  # bismillah
            , ord(u'\ufe0f'): u' '  # blank
            , ord(u'\ufe80'): u'a'  # hamza
            , ord(u'\ufe81'): u'aa'  # alif madda
            , ord(u'\ufe83'): u'a'  # alif circle above
            , ord(u'\ufe84'): u'a'  # -alif hamza above
            , ord(u'\ufe85'): u'o'  # wow hamza above
            , ord(u'\ufe86'): u'u'  # -wow hamza above
            , ord(u'\ufe87'): u'i'  # alif hamza below
            , ord(u'\ufe89'): u'i'  # choti yay hamza above
            , ord(u'\ufe8a'): u'ai'  # -choti yay hamza above
            , ord(u'\ufe8b'): u'i'  # hamza-
            , ord(u'\ufe8c'): u'i'  # -hamza-
            , ord(u'\ufe8d'): u'a'  # alif
            , ord(u'\ufe8e'): u'a'  # -alif
            , ord(u'\ufe8f'): u'b'  # bay
            , ord(u'\ufe90'): u'b'  # -bay
            , ord(u'\ufe91'): u'b'  # bay-
            , ord(u'\ufe92'): u'b'  # -bay-
            , ord(u'\ufe93'): u'ah'  # ha 2 dots above
            , ord(u'\ufe94'): u'ah'  # -ha 2 dots above
            , ord(u'\ufe95'): u't'  # tay
            , ord(u'\ufe96'): u't'  # -tay
            , ord(u'\ufe97'): u't'  # tay-
            , ord(u'\ufe98'): u't'  # -tay-
            , ord(u'\ufe99'): u's'  # say
            , ord(u'\ufe9a'): u's'  # -say
            , ord(u'\ufe9b'): u's'  # say-
            , ord(u'\ufe9c'): u's'  # -say-
            , ord(u'\ufe9d'): u'j'  # jeem
            , ord(u'\ufe9e'): u'j'  # -jeem
            , ord(u'\ufe9f'): u'j'  # jeem-
            , ord(u'\ufea0'): u'j'  # -jeem-
            , ord(u'\ufea1'): u'h'  # hay
            , ord(u'\ufea2'): u'h'  # -hay
            , ord(u'\ufea3'): u'h'  # hay-
            , ord(u'\ufea4'): u'h'  # -hay-
            , ord(u'\ufea5'): u'KH'  # khay
            , ord(u'\ufea6'): u'KH'  # -khay
            , ord(u'\ufea7'): u'KH'  # khay-
            , ord(u'\ufea8'): u'KH'  # -khay-
            , ord(u'\ufea9'): u'd'  # daal
            , ord(u'\ufeaa'): u'd'  # -daal
            , ord(u'\ufeab'): u'z'  # zaal
            , ord(u'\ufeac'): u'z'  # -zaal
            , ord(u'\ufead'): u'r'  # ray
            , ord(u'\ufeae'): u'r'  # -ray
            , ord(u'\ufeaf'): u'z'  # zay
            , ord(u'\ufeb0'): u'z'  # -zay
            , ord(u'\ufeb1'): u's'  # seen
            , ord(u'\ufeb2'): u's'  # -seen
            , ord(u'\ufeb3'): u's'  # seen-
            , ord(u'\ufeb4'): u's'  # -seen-
            , ord(u'\ufeb5'): u'SH'  # sheen
            , ord(u'\ufeb6'): u'SH'  # -sheen
            , ord(u'\ufeb7'): u'SH'  # sheen-
            , ord(u'\ufeb8'): u'SH'  # -sheen-
            , ord(u'\ufeb9'): u's'  # suad
            , ord(u'\ufeba'): u's'  # -suad
            , ord(u'\ufebb'): u's'  # suad-
            , ord(u'\ufebc'): u's'  # -suad-
            , ord(u'\ufebd'): u'z'  # zuad
            , ord(u'\ufebe'): u'z'  # -zuad
            , ord(u'\ufebf'): u'z'  # zuad-
            , ord(u'\ufec0'): u'z'  # -zuad-
            , ord(u'\ufec1'): u't'  # toay
            , ord(u'\ufec2'): u't'  # -toay
            , ord(u'\ufec3'): u't'  # toay-
            , ord(u'\ufec4'): u't'  # -toay-
            , ord(u'\ufec5'): u'z'  # zoay
            , ord(u'\ufec6'): u'z'  # -zoay
            , ord(u'\ufec7'): u'z'  # zoay-
            , ord(u'\ufec8'): u'z'  # -zoay-
            , ord(u'\ufec9'): u'a'  # ain
            , ord(u'\ufeca'): u'a'  # -ain
            , ord(u'\ufecb'): u'a'  # ain-
            , ord(u'\ufecc'): u'a'  # -ain-
            , ord(u'\ufecd'): u'GH'  # ghain
            , ord(u'\ufece'): u'GH'  # -ghain
            , ord(u'\ufecf'): u'GH'  # ghain-
            , ord(u'\ufed0'): u'GH'  # -ghain-
            , ord(u'\ufed1'): u'f'  # fay
            , ord(u'\ufed2'): u'f'  # -fay
            , ord(u'\ufed3'): u'f'  # fay-
            , ord(u'\ufed4'): u'f'  # -fay-
            , ord(u'\ufed5'): u'q'  # qaaf
            , ord(u'\ufed6'): u'q'  # -qaaf
            , ord(u'\ufed7'): u'q'  # qaaf-
            , ord(u'\ufed8'): u'q'  # -qaaf-
            , ord(u'\ufed9'): u'k'  # kaaf
            , ord(u'\ufeda'): u'k'  # -kaaf
            , ord(u'\ufedb'): u'k'  # kaaf-
            , ord(u'\ufedc'): u'k'  # -kaaf-
            , ord(u'\ufedd'): u'l'  # laam
            , ord(u'\ufede'): u'l'  # -laam
            , ord(u'\ufedf'): u'l'  # laam-
            , ord(u'\ufee0'): u'l'  # -laam-
            , ord(u'\ufee1'): u'm'  # meem
            , ord(u'\ufee2'): u'm'  # -meem
            , ord(u'\ufee3'): u'm'  # meem-
            , ord(u'\ufee4'): u'm'  # -meem-
            , ord(u'\ufee5'): u'n'  # noon
            , ord(u'\ufee6'): u'n'  # -noon
            , ord(u'\ufee7'): u'n'  # noon-
            , ord(u'\ufee8'): u'n'  # -noon-
            , ord(u'\ufee9'): u'ah'  # ha
            , ord(u'\ufeea'): u'ah'  # -ha
            , ord(u'\ufeeb'): u'h'  # ha-
            , ord(u'\ufeec'): u'h'  # -double ha-
            , ord(u'\ufeed'): u'w'  # wow
            , ord(u'\ufeee'): u'w'  # -wow
            , ord(u'\ufeef'): u'i'  # choti yay
            , ord(u'\ufef0'): u'i'  # -yay
            , ord(u'\ufef1'): u'y'  # yay 2 dots below
            , ord(u'\ufef2'): u'i'  # -yay
            , ord(u'\ufef3'): u'y'  # yay-
            , ord(u'\ufef4'): u'y'  # -yay-
            , ord(u'\ufefb'): u'la'  # laam-alif
            , ord(u'\ufefc'): u'la'  # -laam-alif
         })


def unicode_to_key(text):
    return re.sub(r'[^a-zA-Z0-9_]+', '', urdu_to_english(text))

# for line in sys.stdin:
#     if line.decode('utf-8') != urdu_to_english(line.decode('utf-8')):
#         unknownchar = False
#         for c in urdu_to_english(line.decode('utf-8')):
#             if ((ord(c) > 200 and ord(c) < 8000) or (ord(c) > 12000 and ord(c) < 55000) or ord(c) > 64000)
#                    and ord(c) not in (65533, 9992):
#                 unknownchar = True
#         if unknownchar or True:
#             for c in line.decode('utf-8'): #urdu_to_english(line.decode('utf-8')):
#                 d = urdu_to_english(c)
#                 if len(d) == 1 and ((ord(d) > 200 and ord(d) < 55000) or ord(d) > 58000):
#                     print "New:",
#                 print c, d, hex(ord(c)), ord(c)
#             print line,
#             print urdu_to_english(line.decode('utf-8')),
#             print unicode_to_key(line.decode('utf-8'))

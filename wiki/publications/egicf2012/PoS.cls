%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%% PoS.cls 1.2.1 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% This file is Copyright PoS 2005:  it can be freely distributed  but
%% NOT modified preserving its name, even in similar forms.
%% 
%% PoS.cls is a LaTeX2e class file used to typeset manuscripts in PoS - 
%% Proceedings of Science style. It was written by P. Dall'Aglio as a 
%% modification of the class `JHEP3.cls'.
%% 
%% For bugs, comments, suggestions email to pos-eo@pos.sissa.it
%% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% \CharacterTable
%%  {Upper-case    \A\B\C\D\E\F\G\H\I\J\K\L\M\N\O\P\Q\R\S\T\U\V\W\X\Y\Z
%%   Lower-case    \a\b\c\d\e\f\g\h\i\j\k\l\m\n\o\p\q\r\s\t\u\v\w\x\y\z
%%   Digits        \0\1\2\3\4\5\6\7\8\9
%%   Exclamation   \!     Double quote  \"     Hash (number) \#
%%   Dollar        \$     Percent       \%     Ampersand     \&
%%   Acute accent  \'     Left paren    \(     Right paren   \)
%%   Asterisk      \*     Plus          \+     Comma         \,
%%   Minus         \-     Point         \.     Solidus       \/
%%   Colon         \:     Semicolon     \;     Less than     \<
%%   Equals        \=     Greater than  \>     Question mark \?
%%   Commercial at \@     Left bracket  \[     Backslash     \\
%%   Right bracket \]     Circumflex    \^     Underscore    \_
%%   Grave accent  \`     Left brace    \{     Vertical bar  \|
%%   Right brace   \}     Tilde         \~}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%  ***:to do  **?:question  ...:to choose  XXX:to delete  ???:unknown %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\NeedsTeXFormat{LaTeX2e}[1995/12/01]
\ProvidesClass{PoS} [2006/05/19 v1.2.1 LaTeX document class - P.D.]

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\def\logo{\raisebox{-5mm}{\hbox{\noindent\includegraphics{PoSlogo}}}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%% NEW SWITCHES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\newif\if@draft   \@draftfalse			% DEFAULT IS NOT DRAFT
\newif\if@hyper   \@hypertrue			% DEFAULT IS HYPER
\newif\if@proc	  \@proctrue			% DEFAULT IS PROC

\newif\if@author\@authorfalse			%
\newif\if@abstract\@abstractfalse		% FOR FRONTMATTER DECLARATIONS
\newif\if@todotoc\@todotocfalse			%
\newif\if@PoScopy\@PoScopyfalse			%
\newif\if@conf\@conffalse			%
\newif\if@shorttitle\@shorttitlefalse		%
\newif\if@PoSspecialurl\@PoSspecialurlfalse
\newif\if@PoScits\@PoScitsfalse			%

\RequirePackage{ifpdf}
%copied from ifpdf.sty
%\newif\ifpdf
%\ifx\pdfoutput\undefined
%\else
%  \ifx\pdfoutput\relax
%  \else
%    \ifcase\pdfoutput
%    \else
%      \pdftrue
%    \fi
%  \fi
%\fi


\if@compatibility
    \setlength\paperheight {297mm}%		% DEFAULT FOR LATEX209 IS a4
    \setlength\paperwidth  {210mm}%
\else						% CHOSE PAPER:
\DeclareOption{a4paper}
   {\setlength\paperheight {297mm}%
    \setlength\paperwidth  {210mm}}
\DeclareOption{a5paper}{\@OImess{}}		% IGNORED a5
\DeclareOption{b5paper}{\@OImess{}}		% IGNORED b5
\DeclareOption{letterpaper}{\@OImess{. PoS pagesize is A4}}
\DeclareOption{legalpaper}{\@OImess{. PoS pagesize is A4}}
\DeclareOption{executivepaper}{\@OImess{. PoS pagesize is A4}}
\DeclareOption{landscape}{\@OImess{}}		% IGNORED landscape
\fi

\newcommand\@ptsize{1}                          % DEFAULT SIZE IS 11 PT

\DeclareOption{proceedings}{\@hypertrue\AtBeginDocument{\@todotocfalse}}

\DeclareOption{hyper}{\@hypertrue\@OImess{ is automatic in PoS}}	% HYPER IS 11 PT
\DeclareOption{nohyper}{\@hyperfalse\PoS@warnl{Option `hyper' should be used}}	% IGNORED nohyper
\DeclareOption{11pt}{\renewcommand\@ptsize{1}\@NFmess{11pt}}%ALWAYS 11PT
\DeclareOption{12pt}{\renewcommand\@ptsize{1}\@NFmess{11pt}}% ALWAYS 11PT
\DeclareOption{10pt}{\renewcommand\@ptsize{1}\@NFmess{11pt}}% ALWAYS 11PT

\DeclareOption{nocits}{\@OImess{ is default for PoS}
                     \@PoScitsfalse}

\DeclareOption{cits}{\@PoScitstrue}
%%%% ??????????????
%come fare per disabilitare cite e forzare cits?
%%%%????????????????????

\if@compatibility\else
\DeclareOption{oneside}{\@twosidefalse \@mparswitchfalse}\fi
\DeclareOption{twoside}{\@OImess{}}
\DeclareOption{notoc}{\AtBeginDocument{\@todotocfalse}
			\@OImess{ is automatic in PoS}}
\DeclareOption{toc}{\AtBeginDocument{\@todotoctrue}}

\DeclareOption{draft}{\setlength\overfullrule{5pt}% EXTENDED DRAFT
	\newcount\hour\newcount\minute
	\hour=\time \divide\hour by60 \minute=\time
	{\multiply\hour by60 \global\advance\minute by-\hour}
	\edef\militarytime{\number\hour:\ifnum\minute<10 0\fi\number\minute}
	\let\old@eqnnum\@eqnnum
	\def\@eqnnum{\old@eqnnum\rlap{\kern\marginparsep
				\if@twocolumn\if@firstcolumn
				  \kern-2\marginparsep\kern-\columnwidth\
				  \llap\bgroup\else\bgroup\fi
				\else
				\if@twoside\ifodd\c@page
				  \kern-2\marginparsep\kern-\textwidth
				  \llap\bgroup\else\bgroup\fi
				\else\bgroup\fi				
				\fi
				\sf\scriptsize\@eqnlabel\egroup}%
				\gdef\@eqnlabel{}}
	\let\@eqnlabel\relax
	\def\math@note#1{\gdef\@eqnlabel{LAB: #1}}
	\def\norm@note#1#2{\special{}% SORRY AGAIN.
			 \ifinner{\ifdim\baselineskip=\z@% IN ARRAY..
					\baselineskip18\p@\fi%%
				\ifhmode%   ATTENTION TO RESTRICTED VMODE!
				\raisebox{.5\baselineskip}[\z@][\z@]{%
					\rlap{\sf\scriptsize #2}}%
				\else\vskip-\baselineskip%
				\raisebox{-.6\baselineskip}[\z@][\z@]{%
						\rlap{\sf\scriptsize #2}}%
				\fi}%
			 \else\marginpar{\raggedright\if@twoside\ifodd\c@page%
					\raggedleft\fi\fi\sf\scriptsize #1#2}%
			 \fi}%
	\AtBeginDocument{\newbox\drft@box\setbox\drft@box=\hbox{%
			\framebox[\textwidth]{\sf\small PoS 3.1 DRAFT 
			\ttfamily(\cur@opt)\hfill\sf\bfseries\jobname\sf, 
			\today, \militarytime}}}
	\@drafttrue}

\DeclareOption{titlepage}{\@OImess{}}		% IGNORED
\if@compatibility\else
\DeclareOption{notitlepage}{\@OImess{}}		% IGNORED
\fi
\if@compatibility\else
\DeclareOption{onecolumn}{\@OImess{}}		% IGNORED
\fi
\DeclareOption{twocolumn}{\@OImess{}}		% IGNORED
\DeclareOption{leqno}{\@OImess{}}	%\input{leqno.clo}} IGNORED ...
\DeclareOption{fleqn}{\@OImess{}}	%\input{fleqn.clo}} IGNORED ...
\DeclareOption{openbib}{\@OImess{}}		% IGNORED


%%%%%%%%%%%%%%%%%%%%%%%%%%%%% MESSAGES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\newcommand\PoS@mess[1]{\typeout{Class PoS Message: \protect#1.}}%
\newcommand\PoS@warn[1]{\ClassWarning{PoS}{#1}}
\newcommand\PoS@warnl[1]{\ClassWarningNoLine{PoS}{#1}}
\newcommand\PoS@igno[1]{\PoS@warn{IGNORED \protect#1}}
\newcommand\PoS@ignol[1]{\PoS@warnl{IGNORED \protect#1}}
\newcommand\@OImess[1]{\PoS@ignol{OPTION '\CurrentOption'#1}}
\newcommand\@NFmess[1]{\PoS@mess{Normalsize set to #1}}
\newcommand\@ENVwarn[1]{\PoS@warn{Please use \protect#1 in place 
						of LaTeX environment}}
%%%%%%%%%%%%%%%%%%%%%%%%%% PROCESS OPTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%
\ExecuteOptions{a4paper,oneside,proceedings} %WERE ALSO 10pt,onecolumn
\ProcessOptions

%%%%%%%%%%%%%%%%%%%%%%%%%% REQUIRED PACKAGES %%%%%%%%%%%%%%%%%%%%%%%%%%
\RequirePackage{color}
\RequirePackage{amssymb}
\RequirePackage[T1]{fontenc}
\RequirePackage{times}
\RequirePackage{mathptmx}
\ifpdf
      \RequirePackage[pdftex]{graphicx}
      \DeclareGraphicsExtensions{.pdf,.jpg,.jpeg}
\else
      \RequirePackage{graphicx}
      \DeclareGraphicsExtensions{.ps,.eps,.pstex}
\fi
%%%%%%%%%%%%%%%%%%%%%%%% TRICKS FOR DOTLESS J %%%%%%%%%%%%%%%%%%%%%%%%%
% \j and \jmath are missing in font Times. So I use the ones from
% Computer Modern. Works acceptably, but in sf
\DeclareSymbolFont{forjmath}{OT1}{cmr}{m}{sl}
\DeclareMathSymbol{\Jmath}{\mathord}{forjmath}{'021}
\def\jmath{\Jmath}
\DeclareFontFamily{OT1}{cmr}{}
\DeclareFontFamily{OT1}{cmss}{}
\def\j{\ifmmode\Jmath\else{\fontencoding{OT1}\fontfamily{cmr}\selectfont\char'021}\fi}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%% END DOTLESS J %%%%%%%%%%%%%%%%%%%%%%%%%%%%



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%% START CODE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\input{size1\@ptsize.clo}
\renewcommand{\baselinestretch}{1.1}\normalsize
\setlength\lineskip{1\p@}
\setlength\parindent{1.2\parindent}
\setlength\normallineskip{1\p@}
\setlength\parskip{0\p@ \@plus \p@}
\@lowpenalty   51
\@medpenalty  151
\@highpenalty 301
\widowpenalty 1000
\clubpenalty 1000

\setcounter{topnumber}{8}
\renewcommand\topfraction{.85} % default .7
\setcounter{bottomnumber}{5}
\renewcommand\bottomfraction{.6} %.2
\setcounter{totalnumber}{8}
\renewcommand\textfraction{.15}
\renewcommand\floatpagefraction{.8}
\setcounter{dbltopnumber}{3}
\renewcommand\dbltopfraction{.85}
\renewcommand\dblfloatpagefraction{.8}
\setlength{\floatsep}{12pt plus 2pt minus 4pt}
\setlength{\textfloatsep}{20pt plus 3pt minus 8pt}
\setlength{\intextsep}{12pt plus 2pt minus 4pt}

\textwidth  .72\paperwidth	% WIDTH OF TEXT, REMAINS .28% FOR MARGINS
%
\setlength\@tempdima{.76\paperheight}	% HEIGHT OF TEXT WITH FOOTNOTES
\divide\@tempdima\baselineskip		% EXCLUDING HEAD (AND FOOT)
\@tempcnta=\@tempdima			% rounded to integer (#lines-1)
\setlength\textheight{\@tempcnta\baselineskip}
\addtolength\textheight{\topskip}

\voffset -1in
\topmargin   .06\paperheight	% FROM TOP OF PAGE TO TOP OF HEADING (0=1inch)
\headheight  .02\paperheight	% HEIGHT OF HEADING BOX.
\headsep     .03\paperheight	% VERT. SPACE BETWEEN HEAD AND TEXT.
\footskip    .04\paperheight	% FROM END OF TEX TO BASE OF FOOTER. (40pt)

\marginparsep 9\p@		% HOR. SPACE BETWEEN TEXT AND MARGINAL NOTE
\marginparpush 6\p@		% MIN. VERT. SPACE BETWEEN MARGINAL NOTES

\hoffset -1in				% TO ADJUST WITH PAPER:

	\oddsidemargin .14\paperwidth	% LEFT MARGIN FOR ODD PAGES (20)
	\evensidemargin .14\paperwidth	% LEFT MARGIN FOR EVEN PAGES (20)
	\marginparwidth .11\paperwidth	% TEXTWIDTH OF MARGINALNOTES

%%%%%%%%%%%%%%%%%%%%%%% HYPER (AND DRAFT) STUFF %%%%%%%%%%%%%%%%%%%%%%%
%\let\textref\@gobble
\ifpdf
      \input pdfcolor.tex
\else
\fi

\bgroup\catcode`\#=12\gdef\hash{#}\egroup	% DEFINED \hash=#.
\def\H@tilde{\string~}
\newcommand{\href}[2]{\rule{0pt}{0pt}\bgroup\let~\H@tilde
		      \if@hyper\ifpdf
				    \def\hash{}%
				    \pdfstartlink attr{/Border [0 0 0]}
                                    user{/Subtype /Link /A << /S /URI /URI (#1) >>}%
					\pdfsetcolor{.9 .9 0 0}%
				\else
				    \noexpand\special{html:<A href="#1">}%
				\fi
		      \fi
		      {#2}\rule{0pt}{0pt}\egroup
		      \if@hyper\ifpdf
				    \Black\pdfendlink
			       \else
				    \special{html:</A>}%
			       \fi
                      \fi
		      }
% \intPlink was added because in pdflatex you need two different commands
% for internal and external links. For latex they are the same.
\newcommand{\intPlink}[2]{\rule{0pt}{0pt}\bgroup\let~\H@tilde
                        \if@hyper\ifpdf
                                       \def\hash{}%
                                       \pdfstartlink attr{/Border [0 0 0]}
                                          goto name{#1}%
                                       \pdfsetcolor{.9 .9 0 0}%
				  \else
                                       \noexpand\special{html:<A href="#1">}%
				  \fi
			\fi
                        {#2}\rule{0pt}{0pt}\egroup
                        \if@hyper\ifpdf
                                      \Black\pdfendlink
				 \else
                                      \special{html:</A>}%
                                 \fi
                        \fi
                        }


\newcommand{\name}[1]{\if@hyper\ifpdf
				    \pdfdest name{#1} fith%
			       \else
				    \noexpand\special% NAME HAS NO TAGGED TEXT HERE.
					{html:<A name="#1">}\special{html:</A>}%
			       \fi
		      \fi
		      }

\newcommand{\base}[1]{\if@hyper\bgroup\let~\H@tilde
			\noexpand\special	% BASE HAS NO TAGGED TEXT.
			{html:<base href="#1">}\egroup\fi}
%------------------------------------------------------------------%
% SECTION TAGS and  THEIR REFERENCE IN TOC \d@t will be '.' for sections only
\if@hyper
\renewcommand{\@seccntformat}[1]{\name{sec\csname the#1\endcsname}%
				 \csname the#1\endcsname\d@t\hspace{1ex}}
\renewcommand{\numberline}[1]{\hb@xt@\@tempdima{\intPlink
			      {\hash\hyp@typ#1}{#1}\d@t\hfil}}
\else
\renewcommand{\@seccntformat}[1]{\csname the#1\endcsname\d@t\hspace{1ex}}
\renewcommand{\numberline}[1]{\hb@xt@\@tempdima{#1\d@t\hfil}}
\fi
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\if@PoScits
% if you want citations ranges to collapse...
\newcount\@tempcntc
\def\@citex[#1]#2{%
  \@tempcnta\z@\@tempcntb\m@ne\def\@citea{}%
  \@cite{\@for\@citeb:=#2\do%                                               for aech citation...
         {\edef\@citeb{\expandafter\@firstofone\@citeb}%                    eat trailing space
          \if@filesw\immediate\write\@auxout{\string\citation{\@citeb}}\fi% write to aux
          \@ifundefined{b@\@citeb}%                                         if label undefined..
                       {\@citeo\@tempcntb\m@ne\@citea%                      ... then ...
                        \def\@citea{,\penalty\@m\ }{\reset@font\bfseries ?}%
	                \G@refundefinedtrue\@latex@warning%
                        {Citation `\@citeb' on page \thepage \space undefined}}%
                       {\setbox\z@\hbox{\global\@tempcntc0\csname %         ... else
                                        b@\@citeb\endcsname}%
                        \advance\@tempcntb\@ne%
                        \ifnum\@tempcntb=\@tempcntc%
                        \else\advance\@tempcntb\m@ne\@citeo%
                             \@tempcnta\@tempcntc\@tempcntb\@tempcntc
                        \fi%
                        }%
         }\@citeo}{#1}%
	 \if@draft\norm@note{CIT: }{#2}\fi			% **DRAFT**
    }% end definition of \@citex
%This really prints out the cit-numbers
\def\@citeo{\ifnum\@tempcnta>\@tempcntb%    
            \else\@citea\def\@citea{,\penalty\@m\ }%
                  \ifnum\@tempcnta=\@tempcntb%
                  \intPlink{\hash bib\the\@tempcnta}{\the\@tempcnta}% if there is no range
                  \else{\advance\@tempcnta\@ne%                 % if there is a range
                       \ifnum\@tempcnta=\@tempcntb%
                       \else\def\@citea{\,--\,}%
                       \fi%
                       \advance\@tempcnta\m@ne%
                       \intPlink{\hash bib\the\@tempcnta}{\the\@tempcnta}\@citea%first
                       \intPlink{\hash bib\the\@tempcntb}{\the\@tempcntb}}%last
                  \fi%
             \fi}
\else % ... or not to collapse
%------------------------------------------------------------------%
% CITATION TAGS ARE TOGETHER WITH BIB STUFF.
% CITATION HREF
\def\@citex[#1]#2{%			% UNFORTUNATELY REDEFINED!!!!
  \let\@citea\@empty
  \@cite{\@for\@citeb:=#2\do
    {\@citea\def\@citea{,\penalty\@m\ }%
     \edef\@citeb{\expandafter\@firstofone\@citeb}%
     \if@filesw\immediate\write\@auxout{\string\citation{\@citeb}}\fi
     \@ifundefined{b@\@citeb}{\mbox{\reset@font\bfseries ?}%
	\G@refundefinedtrue
	\@latex@warning
	  {Citation `\@citeb' on page \thepage \space undefined}}%
	{\edef\tmp@bn{\csname b@\@citeb\endcsname}%
	 \hbox{\intPlink{\hash bib\tmp@bn}{\tmp@bn}}}}}{#1}%		% **HYPER**
	 \if@draft\norm@note{CIT: }{#2}\fi			% **DRAFT**
	 }
\fi
%-----------------------------------------------------------------%
% LABEL, REF AND PAGEREF, TEXTREF, ~ COMPATIBLE:
\let\old@label\label						% OLD LATEX COMMAND.
\def\label{\bgroup\catcode`\>=15\catcode`\"=15\catcode`\==15 \@@label}		
\newcommand{\@@label}[1]{\egroup\name{ref-#1}%		% LABEL IN HYPER TAG.
			 \old@label{#1}%		% 
			 \if@draft%			% DRAFT:
			 \ifmmode\math@note{#1}%
			 \else\norm@note{}{LAB: #1}\fi%
			 \fi%
			 }
\if@hyper
\let\old@ref\ref \let\old@pageref\pageref		% OLD LATEX COMMANDS.
\renewcommand{\ref}[1]{\@ifundefined{r@#1}{}{\intPlink{\hash ref-#1}}%
		       {\old@ref{#1}}} % LABEL INSIDE HREF.
\renewcommand{\pageref}[1]{\@ifundefined{r@#1}{}{%	% UNDEFINED => NO HREF
	      \edef\tmp@ref{\noexpand\@secondoftwo\csname r@#1\endcsname}%
	      \intPlink{\hash pag\tmp@ref}}{\old@pageref{#1}}}
\fi
\newcommand{\textref}[2]{\@ifundefined{r@#1}
			{%
			\protect\G@refundefinedtrue
			\nfss@text{\reset@font\bfseries ?#2?}%
   			\@latex@warning{Reference `#1' on page \thepage \space
				             undefined}}
			{\vrule height \z@ width \z@\intPlink{\hash ref-#1}{#2}}}
%------------------------------------------------------------------%
%%%%%%%%%%%%%%%%%%%%%%%%%% END HYPER-STUFF %%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%% PoS HEADINGS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\def\ps@PoS{% 			
    \def\@oddfoot{\ifnum\thepage=1%
		\PoScopyright@box\hfill% copyright only on front page
		\if@PoSspecialurl\PoSspecial@url\else\unhbox\PoSpaper@url\fi%possible url
		\else\reset@font\hfil\thepage\hfil\fi} % page numbers on other pages
    \if@draft\edef\cur@opt{\@curroptions}\fi
%
    \def\@oddhead{\name{pag\thepage}\ifnum\thepage=1%
      {\smash{\makebox[0cm][l]{\logo}}\hfill}\else%
      \smash{\if@draft\raise 2.5em\rlap{\copy\drft@box}\fi
		     \vbox{\hsize=\textwidth\noindent
		     \if@shorttitle\copy\PoSshorttitle@box\else{\small\it\@title}\fi\hfill
		     \copy\@firstaubox\vskip.17em \hrule
             }}\fi%
		      }%
    \let\@mkboth\@gobbletwo  %%%% 
    \let\sectionmark\@gobble
    \let\subsectionmark\@gobble
    }
%%%%%%%%%%%%%%%%%%%%%%%%%%%%% TITLE PAGE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\def\maketitle{\PoS@ignol{\maketitle}}
\newcommand\auto@maketitle{\begingroup
    \renewcommand\thefootnote{\@fnsymbol\c@footnote}% 
    \def\@makefnmark{\rlap{\@textsuperscript{\normalfont\@thefnmark}}}% 
    \long\def\@makefntext##1{\parindent 20.39995pt\noindent
            \hb@xt@.05\textwidth{\hss\@textsuperscript{\normalfont\@thefnmark}}##1}%
    \let\footnote\thanks%
	 \@maketitle				% CALL ACTUAL FORMATTING
	 \@thanks				% TITLE THANKS
	 \thispagestyle{PoS} 
 	 \setcounter{page}{1}
	 \newpage				% END OF PAGE
  \endgroup
  \setcounter{footnote}{0}%			
  \if@todotoc					% IF THERE IS A SECTION
      \noindent\rule\textwidth{.1pt}		% THEN MAKE TOC...
      \vskip 2em \@plus 3ex \@minus 3ex
      \tableofcontents
      \vskip 2em \@plus 3ex \@minus 3ex
      \noindent\rule\textwidth{.1pt}
      \vskip 2em \@plus 3ex \@minus 3ex
  \fi
  \global\let\thanks\@gobble
  \global\let\@maketitle\relax			%
  \global\let\@thanks\@empty			%
  \global\let\@author\@empty			% KILL ALL 
  \global\let\@date\@empty			%
  \global\let\@abstract\@empty			%
  \global\let\title\relax			%
  \global\let\author\relax			%
  \global\let\date\relax			%
  \global\let\and\relax				%
  \global\let\email\@gobble
  \global\let\PoScopydate\relax
  \global\let\PoSspecialurl\relax
}
%%%%%%%%%%%%%%%%%%%%%%%%%% ACTUAL TITLEPAGE %%%%%%%%%%%%%%%%%%%%%%%%%%%
\newskip\titlesk@a		% Skips for titlepage
\newskip\titlesk@b		%
\newskip\titlesk@c		%
\titlesk@a= 2em   plus .4fill  minus 1em%	% Before title (proceedings)
\titlesk@b= 1.7em plus .1fill  minus 1em%	% Between title/rule & rule/auth
\titlesk@c= 1.5em plus .05fill minus 1em%	% Between auth/abs & abs/dedic

\def\@maketitle{%				% PAGE IS FLUSHED LEFT
  \begin{flushleft}%				% PAY ATTENTION TO \par'S
    \null
    %
    \vskip \titlesk@a		   % vspace before title
    %
    {\LARGE \sffamily              % TITLE  
      \bfseries                  
      \@title\par}%                
    %
    \vskip \titlesk@b              % vspace between title and rule
    %
    \hrule height 1\p@%            % RULE  1pt.
    %
    \vskip \titlesk@b		   % vspace between rule and authors
    %
    {\normalsize \bfseries \sffamily% AUTHOR(S)
      \@author \par}                            
    %
    \vskip \titlesk@c              % vspace between authors and abstract
    %
    \centerline{\parbox{.9\textwidth}%	% ABSTARCT (narrower)
      {\abstract@cs}}%
    %
    \dedic@ted                     % DEDICATION (if present) with its space
    %
    \@PACS                         % PACS (if present) with its space
    %
    \vskip \titlesk@c            
    \vfill
    \FullConference@box            % FULL CONFERENCE INFO
  \end{flushleft}%
}
%%%%%%%%%%%%%%%%%%%%%%%%%%% STANDARD STUFF %%%%%%%%%%%%%%%%%%%%%%%%%%%%
\setcounter{secnumdepth}{3}
\newcounter {part}
\newcounter {section}
\newcounter {subsection}[section]
\newcounter {subsubsection}[subsection]
\newcounter {paragraph}[subsubsection]
\newcounter {subparagraph}[paragraph]
\renewcommand\thepart         {\@Roman\c@part}
\renewcommand\thesection      {\@arabic\c@section}
\renewcommand\thesubsection   {\thesection.\@arabic\c@subsection}
\renewcommand\thesubsubsection{\thesubsection.\@arabic\c@subsubsection}
\renewcommand\theparagraph    {\thesubsubsection.\@arabic\c@paragraph}
\renewcommand\thesubparagraph {\theparagraph.\@arabic\c@subparagraph}
%%%%%%%%%%%%%%%%%%%%%%%%%%% SECTION FORMATS %%%%%%%%%%%%%%%%%%%%%%%%%%%
\def\ts@flag{\let\d@t.%					% Sections get ``.''
	     \immediate\write\@auxout			% IF THERE IS A SECTION
	     {\string\global\string\@todotoctrue}}	% THEN SIGNAL IN AUX.

\newcommand\part{\par
   \addvspace{4ex}%
   \@afterindentfalse
   \secdef\@part\@spart}

\def\@part[#1]#2{\ts@flag
    \ifnum \c@secnumdepth >\m@ne
      \refstepcounter{part}%			% DAMN LATEX !!! BELOW,
      \addcontentsline{toc}{part}{\string\intPlink	% UNFORTUNATELY, NO NUMBERLINE:
				  {\string\hash\space part\the\c@part}%
				  {\thepart}\hspace{1em}#1}%
    \else
      \addcontentsline{toc}{part}{#1}%
    \fi
    {\parindent \z@ \raggedright
     \interlinepenalty \@M
     \normalfont
     \ifnum \c@secnumdepth >\m@ne
       \name{part\the\c@part}\Large\bfseries \partname~\thepart
       \par\nobreak
     \fi
     \huge \bfseries #2%
     \markboth{}{}\par}%
    \nobreak
    \vskip 3ex
    \@afterheading\let\d@t\relax}
\def\@spart#1{%
    {\parindent \z@ \raggedright
     \interlinepenalty \@M
     \normalfont
     \huge \bfseries #1\par}%
     \nobreak
     \vskip 3ex
     \@afterheading}

\newcommand\secstyle{\bfseries\raggedright}

\newcommand\section{\@startsection{section}{1}{\z@}%
                                   {3.5ex \@plus 1.3ex \@minus .7ex}%
                                   {2.3ex \@plus.4ex \@minus .4ex}%
                                   {\normalfont\large\secstyle}}
\newcommand\subsection{\@startsection{subsection}{2}{\z@}%
                                   {2.3ex\@plus 1ex \@minus .5ex}%
                                   {1.2ex \@plus .3ex \@minus .3ex}%
                                   {\normalfont\normalsize\secstyle}}
\newcommand\subsubsection{\@startsection{subsubsection}{3}{\z@}%
                                   {2.3ex\@plus 1ex \@minus .5ex}%
                                   {1ex \@plus .2ex \@minus .2ex}%
                                   {\normalfont\normalsize\secstyle}}
\newcommand\paragraph{\@startsection{paragraph}{4}{\z@}%
                                   {1.75ex \@plus1ex \@minus.2ex}%
                                   {-1em}%
                                   {\normalfont\normalsize\bfseries}}
\newcommand\subparagraph{\@startsection{subparagraph}{5}{\parindent}%
                                   {1.75ex \@plus1ex \@minus .2ex}%
                                   {-1em}%
                                   {\normalfont\normalsize\bfseries}}

% ---- turnaround for sections with figures ------ %
\def\bef@sec{\iffigprocessing\PoS@warnl{Floating figure \the\ffigcount\space
						and startsection colliding}\fi
	     \iftabprocessing\PoS@warnl{Floating table \the\ftabcount\space
						and startsection colliding}\fi
	     \edef\bef@everypar{\the\everypar}}	% HACK FOR FLOATS AND \everypar
\def\aft@sec{\let\d@t\relax			% RESTORE: NO DOTTED SUBSECS..
	     \edef\tmp@everypar{\the\everypar}% % DELETED CLUBPENALTY, BELOW.
	     \everypar\expandafter{\bef@everypar{\tmp@everypar}}}% RESTORE.
\let\old@sec\section				%
\let\old@ssec\subsection			%
\let\old@sssec\subsubsection			% OLD SECTIONING COMMANDS.
\let\old@par\paragraph				%
\let\old@spar\subparagraph			%
% ------- new forms ------- %
\renewcommand{\section}{\secdef\PoS@sec\PoS@secs}
\renewcommand{\subsection}{\secdef\PoS@ssec\PoS@ssecs}
\renewcommand{\subsubsection}{\secdef\PoS@sssec\PoS@sssecs}
\renewcommand{\paragraph}{\secdef\PoS@par\PoS@pars}
\renewcommand{\subparagraph}{\secdef\PoS@spar\PoS@spars}
% ---- unstarred forms ---- %
\def\PoS@sec[#1]#2{\ts@flag\bef@sec\old@sec[#1]{#2}\aft@sec}
\def\PoS@ssec[#1]#2{\bef@sec\old@ssec[#1]{#2}\aft@sec}
\def\PoS@sssec[#1]#2{\bef@sec\old@sssec[#1]{#2}\aft@sec}
\def\PoS@par[#1]#2{\bef@sec\old@par[#1]{#2}\aft@sec}
\def\PoS@spar[#1]#2{\bef@sec\old@spar[#1]{#2}\aft@sec}
% ----- starred forms ----- %
\def\PoS@secs#1{\bef@sec\old@sec*{#1}\aft@sec}
\def\PoS@ssecs#1{\bef@sec\old@ssec*{#1}\aft@sec}
\def\PoS@sssecs#1{\bef@sec\old@sssec*{#1}\aft@sec}
\def\PoS@pars#1{\bef@sec\old@par*{#1}\aft@sec}
\def\PoS@spars#1{\bef@sec\old@spar*{#1}\aft@sec}
% ----- end hack for sections with floats --------%

\setlength\leftmargini  {2.5em}
\leftmargin  \leftmargini
\setlength\leftmarginii  {2.2em}
\setlength\leftmarginiii {1.87em}
\setlength\leftmarginiv  {1.7em}
\setlength\leftmarginv  {1em}
\setlength\leftmarginvi {1em}
\setlength  \labelsep  {.5em}
\setlength  \labelwidth{\leftmargini}
\addtolength\labelwidth{-\labelsep}
\@beginparpenalty -\@lowpenalty
\@endparpenalty   -\@lowpenalty
\@itempenalty     -\@lowpenalty
\renewcommand\theenumi{\@arabic\c@enumi}
\renewcommand\theenumii{\@alph\c@enumii}
\renewcommand\theenumiii{\@roman\c@enumiii}
\renewcommand\theenumiv{\@Alph\c@enumiv}
\newcommand\labelenumi{\theenumi.}
\newcommand\labelenumii{(\theenumii)}
\newcommand\labelenumiii{\theenumiii.}
\newcommand\labelenumiv{\theenumiv.}
\renewcommand\p@enumii{\theenumi}
\renewcommand\p@enumiii{\theenumi(\theenumii)}
\renewcommand\p@enumiv{\p@enumiii\theenumiii}
\newcommand\labelitemi{$\m@th\bullet$}
\newcommand\labelitemii{\normalfont\bfseries --}
\newcommand\labelitemiii{$\m@th\ast$}
\newcommand\labelitemiv{$\m@th\cdot$}
\newenvironment{description}
               {\list{}{\labelwidth\z@ \itemindent-\leftmargin
                        \let\makelabel\descriptionlabel}}
               {\endlist}
\newcommand*\descriptionlabel[1]{\hspace\labelsep
                                \normalfont\bfseries #1}
%%%%%%%%%%%%%%%%%%%%%%%%%% AUTHORS/ADDRESSES %%%%%%%%%%%%%%%%%%%%%%%%%%
% *** After proceedings, the authors go in a hbox. => \break does not work!!
% *** if no address nobody closes the box!
\newif\if@speaker\@speakerfalse
\newif\if@dblspeak\@dblspeakfalse
\newtoks\prev@t
\newtoks\cur@t
\newbox\@firstaubox
\newskip\addr@skip\addr@skip=.38em%\@plus.02fil\@minus.3ex
\newskip\auth@skip\auth@skip=.6em\@plus.03fil\@minus.2ex

\renewcommand\@author{\parskip\z@
		      \def\\{\egroup			% CLOSE & aftergroup!
			\par				% FIRST TIME \\=>ADDR.
			\vskip\addr@skip		% GLUE UNDER AUTHOR
			\vbox\bgroup\hsize=.9\textwidth	% 
			\small\it\raggedright		% STYLE FOR ADDRESSES
			\def\\{\par\leavevmode\ignorespaces}%\let\\\relax%
			\leavevmode\ignorespaces}%
			}
\renewcommand\author[1]{%
		\global\@authortrue			%
		\prev@t=\expandafter{\@author}%		% TWO TOKEN LISTS.
		\cur@t={\vskip\auth@skip		% glue before auth.
			\vtop\bgroup #1\egroup		% ACTUAL AUTH.\\ADD.
			\par}%				%
		\long\xdef\@author{\the\prev@t\the\cur@t}%STORE ALL IN \@AUTHOR
		}
%%%%%%%%%%%%%%%%%%%%%%%%%%%% SPEAKER %%%%%%%%%%%%%%%%%%%%%%%%%%%%
\newcommand\speaker[1]{\if@speaker\global\@dblspeaktrue\fi
			\global\@speakertrue
			\global\setbox\@firstaubox
			\hbox{{\let\thanks\@gobble
				\let\footnote\@gobble\small 
				\rm #1}}%
			#1\thanks{Speaker.}\
			}%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%% SHORT TITLE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\newbox\PoSshorttitle@box
\newcommand\ShortTitle[1]{\global\setbox\PoSshorttitle@box
                          \hbox{\small\it #1}\global\@shorttitletrue}
%%%%%%%%%%%%%%%%%%%%%%%%%% Full Conference Info %%%%%%%%%%%%%%%%%%%%%%%
\gdef\FullConference@box{\relax}
\newcommand\FullConference[1]{\gdef\FullConference@box{
			 \vbox{\small\raggedright
			   \it #1}}\@conftrue}
%%%%%%%%%%%%%%%%%%%%%%%%%% COPYRIGHT DATE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\newbox\PoScopy@date
\newcommand\PoScopydate[1]{\global\setbox\PoScopy@date
                        \hbox{\scriptsize #1}\global\@PoScopytrue}
%%%%%%%%%%%%%%%%%%%%%%%%%%% ACKNOWELEDGMENTS %%%%%%%%%%%%%%%%%%%%%%%%%%
\newcommand\acknowledgments{\section*{\acknowlname}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% EMAIL %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\newcommand\email[1]{{\tt\href{mailto:#1}{#1}}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% DEDICATED %%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\gdef\dedic@ted{\relax}
\newcommand\dedicated[1]{\gdef\dedic@ted{\vskip .5\titlesk@c
                              \vbox{\small\it\raggedleft #1}}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PACS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\gdef\@PACS{\relax}
\newcommand\PACS[1]{\gdef\@PACS{\vskip.5\titlesk@c
                                \vbox{\small\tt\raggedleft PACS:\ #1}}} 
%%%%%%%%%%%%%%%%% SPECIAL URL overrides paper@url %%%%%%%%%%%%%%%%%%%%%
\gdef\PoSspecial@url{\relax}
\newcommand\PoSspecialurl[1]{\@PoSspecialurltrue\gdef\PoSspecial@url{%
    \hbox{\tiny\tt #1}}}
%%%%%%%%%%%%%%%%%%%%%%%%%  URL %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\newbox\PoSpaper@url
\global\setbox\PoSpaper@url\hbox{\tiny\ttfamily 
		http://pos.sissa.it/}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% COPYRIGHT %%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\gdef\PoScopyright@box{\hbox{{\tiny $\copyright$ Copyright owned by
the author(s) under the terms of the Creative Commons
Attribution-NonCommercial-ShareAlike Licence.}}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% ABSTRACT %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\newcommand{\abstract}[1]%		% ABSTR. IN NORMALSIZE
	{\gdef\abstract@cs{		% cs FOR WHOLE ABSTRACT
	{\small
	  \ignorespaces #1}}
	\normalsize
	\global\@abstracttrue}
%%%%%%%%%%%%%%%%%%%%%%%%%% OTHER ENVIRONMENTS %%%%%%%%%%%%%%%%%%%%%%%%%
\newenvironment{verse}
               {\let\\\@centercr
                \list{}{\itemsep      \z@
                        \itemindent   -1.5em%
                        \listparindent\itemindent
                        \rightmargin  \leftmargin
                        \advance\leftmargin 1.5em}%
                \item\relax}
               {\endlist}
\newenvironment{quotation}
               {\list{}{\listparindent 1.5em%
                        \itemindent    \listparindent
                        \rightmargin   \leftmargin
                        \parsep        \z@ \@plus\p@}%
                \item\relax}
               {\endlist}
\newenvironment{quote}
               {\list{}{\rightmargin\leftmargin}%
                \item\relax}
               {\endlist}

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% THEOREMS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\def\@xthm#1#2{\@begintheorem{#2}		 % theorems are numbered
		{\csname the#1\endcsname.}	 % with a . after the number
		\ignorespaces}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% APPENDIX %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\newcommand\appendix{\par
  \setcounter{section}{0}%
  \setcounter{subsection}{0}%
  \renewcommand\thesection{\@Alph\c@section}}
%%%%%%%%%%%%%%%%%%%%%%%%%%% SOME MORE LENGHTS %%%%%%%%%%%%%%%%%%%%%%%%%
\setlength\arraycolsep{2\p@}		% TO MATCH eqnarrays<->equations
\setlength\tabcolsep{6\p@}
\setlength\arrayrulewidth{.4\p@}
\setlength\doublerulesep{2\p@}
\setlength\tabbingsep{\labelsep}
\skip\@mpfootins = \skip\footins
\setlength\fboxsep{3\p@}
\setlength\fboxrule{.4\p@}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%% THE EQUATION %%%%%%%%%%%%%%%%%%%%%%%%%%%%
\renewcommand\theequation{\ifnum\c@section=0\else\thesection.\fi
			  \@arabic\c@equation}

%%%%%%%%%%%%%%%%%%%%%%%%%%%% FIGURES, TABLES %%%%%%%%%%%%%%%%%%%%%%%%%%
\newcounter{figure}
\renewcommand\thefigure{\@arabic\c@figure}
\def\fps@figure{Htbp}
\def\ftype@figure{1}
\def\ext@figure{lof}
\def\fnum@figure{\figurename~\thefigure}
\newenvironment{figure}
               {\@float{figure}}
               {\end@float}
\newenvironment{figure*}
               {\@dblfloat{figure}}
               {\end@dblfloat}
\newcounter{table}
\renewcommand\thetable{\@arabic\c@table}
\def\fps@table{tbp}
\def\ftype@table{2}
\def\ext@table{lot}
\def\fnum@table{\tablename~\thetable}
\newenvironment{table}
               {\@float{table}}
               {\end@float}
\newenvironment{table*}
               {\@dblfloat{table}}
               {\end@dblfloat}
%%-----------------------------------------------------------------%%
\newcommand{\FIGURE}[2][v]{\begin{floatingfigure}[#1]#2
                                        \end{floatingfigure}}
\newcommand{\TABLE}[2][v]{\begin{floatingtable}[#1]{#2}
                                        \end{floatingtable}}
\newcommand{\EPSFIGURE}[3][v]{\begin{floatingfigure}[#1]\epsfig{file=#2}
                                        \caption{#3}\end{floatingfigure}}
\newcommand{\TABULAR}[4][v]{\begin{floatingtable}[#1]{\begin{tabular}{#2}
					#3\end{tabular}}\caption{#4}
                                        \end{floatingtable}}
%Check below, and put automatic width ***
\newcommand{\DOUBLEFIGURE}[5][ht]{\@dblfloat{figure}[#1]\centerline{%
		\parbox{.45\textwidth}{\centerline{\epsfig{file=#2}}}~~~~
		\parbox{.45\textwidth}{\centerline{\epsfig{file=#3}}}}
		\centerline{\parbox[t]{.45\textwidth}{\caption{#4}}~~~~
		\parbox[t]{.45\textwidth}{\caption{#5}}}\end@dblfloat}
\newcommand{\DOUBLETABLE}[5][ht]{\@dblfloat{table}[#1]\centerline{%
		\parbox{.45\textwidth}{\centerline{#2}}~~~~
		\parbox{.45\textwidth}{\centerline{#3}}}
		\centerline{\parbox[t]{.45\textwidth}{\caption{#4}}~~~~
		\parbox[t]{.45\textwidth}{\caption{#5}}}\end@dblfloat}
%%%%%%---------------- FROM FLOATFLT PACKAGE ------------------%%%%%%%%
%% Original file `floatflt.sty', modified by F.Nesti  to:
%% be inserted in PoS.cls;
%% correct float placement when shifting past pages;
%% correct incompatibility with \marginpar;
%% correct incompatibility with \@startsection;
%% correct persisting indentation in following pars;
%% let float also at beginning of pages; 
%% avoid the widht specification.
%% 
%% Original was 1994-1996 by Mats Dahlgren <matsd@physchem.kth.se>. 
%% 
%%%%%%%%\NeedsTeXFormat{LaTeX2e}[1994/06/01]		%LEFT THIS STUFF TO 
%%%%%%%%\ProvidesPackage{floatflt}[1996/02/27 v. 1.3]	%RECONVERT TO PACKAGE.
\newcounter{OptionTest}
\if@twoside
 \setcounter{OptionTest}{0}
\else							% POS: 2side=p/1side=r
 \setcounter{OptionTest}{1}
\fi
%%%%%%%%\DeclareOption{rflt}{\setcounter{OptionTest}{1}}
%%%%%%%%\DeclareOption{lflt}{\setcounter{OptionTest}{2}} % CHOSEN.
%%%%%%%%\DeclareOption{vflt}{\setcounter{OptionTest}{0}}
%%%%%%%%\DeclareOption*{\OptionNotUsed}
%%%%%%%%\ProcessOptions
\newbox\@tmpbox
\newbox\figbox
\newbox\tabbox
\newbox\pagebox
\newcount\ffigcount
\newcount\ftabcount
\newcount\hangcount
\newcount\nosuccesstryfig
\newcount\nosuccesstrytab
\newdimen\figgutter   \figgutter=1truepc
\newdimen\tabgutter   \tabgutter=1truepc
\newdimen\fl@wd
\newdimen\fl@ht
%\newdimen\fl@gut
\newdimen\htdone      \htdone=\z@
\newdimen\pageht
\newdimen\startpageht
\newdimen\floatfltwidth
\newdimen\fltitemwidth
\newif\iftryingfig     \tryingfigfalse
\newif\iftryingtab     \tryingtabfalse
\newif\ifdoingfig      \doingfigfalse
\newif\ifdoingtab      \doingtabfalse
\newif\iffigprocessing \figprocessingfalse
\newif\iftabprocessing \tabprocessingfalse
\newif\ifpageafterfig  \pageafterfigfalse
\newif\ifpageaftertab  \pageaftertabfalse
\newif\ifoddpages
\newif\ifoutput
\newtoks\outputpretest
\def\@captype{}
%%---------------- ORRIBLE HACKS, SORRY -------------------------%%
\let\old@marginpar\marginpar				% 
\renewcommand\marginpar[1]{{\outputpretest={\outputtrue}% REDEF \marginpar.
			    \old@marginpar{#1}}}	%
\def\g@addto#1#2{{\toks@\expandafter{#1#2}%
		  \xdef#1{\the\toks@}}}%
\def\postpone@captions{%			%
%	\global\let\tmp@label\label		% REDEF \caption AND 
	\global\let\tmp@caption\caption		% \label, LATER.
	\global\let\later@capt\relax		% UN PO' PESANTI 
	\gdef\later@label{}%			% MA D'ALTRONDE...
	\let\J@ref\ref\def\ref{\protect\J@ref}% % protect ref in captions.. 
	\renewcommand\caption{\@ifnextchar[\J@@caption\J@caption}%
	\newcommand\J@caption[1]{\gdef\later@capt{\tmp@caption{##1}}}% LOCAL!!
	\newcommand\J@@caption[2][v]%
				{\gdef\later@capt{\tmp@caption[##1]{##2}}}% LOCAL!!
	\renewcommand\@@label[1]{\egroup\name{ref-##1}%	% TAG WHERE CALLED, AT LEAST,
			       \if@draft\norm@note{}{LAB: ##1}\fi% & DRAFTNOTE.
			       \g@addto\later@label{\old@label{##1}}}%
			}
\AtEndDocument{\iftryingfig\PoS@warnl{Floating figure \the\ffigcount\space
					remains undone}\fi
		\iftryingtab\PoS@warnl{Floating table \the\ftabcount\space
					remains undone}\fi}
%%----------------- FIGURE ---------------------------------------%%
\newenvironment{floatingfigure}[1][v]%
{\@tfor \@tempa :=#1\do {\xdef\@fside{\@tempa}}%
 \global\advance\ffigcount by 1%
 \iffigprocessing {\count0=\ffigcount\advance\count0 by -1%
   		   \PoS@warnl{Floating figures \the\count0\space\space and
       			\the\ffigcount\space colliding}}\fi
 \iftabprocessing  \PoS@warnl{Floating table \the\ftabcount\space and 
			floating figure \the\ffigcount\space colliding}\fi
 \postpone@captions
 \global\setbox\@tmpbox=\hbox\bgroup% begin of figbox %HBOX FOR AUTO WIDTH!
}
{%
\egroup%				% CALCULATE WIDTH (NO CAPTION FOR NOW)
\global\setlength{\floatfltwidth}{\the\wd\@tmpbox}%
\ifdim\floatfltwidth<.6\columnwidth%	% IF NOT TOO WIDE OK:
 \global\setbox\figbox=\vbox{\hsize=\floatfltwidth
			     \def\@captype{figure}%
			     \noindent\unhbox\@tmpbox
			     \later@capt\later@label}%
 \global\figprocessingtrue
 \global\everypar={\tryfig\oldeverypar}% must be set globally!
 \figinsert\par
\else%					% ELSE NORMAL LATEX FIGURE, SIGH.
 \if\@fside v \def\@fside{ht}\fi
 \ifdim\floatfltwidth<\columnwidth
    \def\@@tempa{\@float{figure}[}\let\@@tempb\end@float
 \else
    \def\@@tempa{\@dblfloat{figure}[}\let\@@tempb\end@dblfloat
 \fi
 \expandafter\@@tempa\@fside]\center\unhbox\@tmpbox\later@capt\later@label\@@tempb
\fi
}
%%------------ TABLE ----------------------------------------------%%
\newenvironment{floatingtable}[1][v]%
{\@tfor \@tempa :=#1\do {\global\edef\@tside{\@tempa}}%
 \global\advance\ftabcount by 1%
 \iftabprocessing {\count0=\ftabcount\advance\count0 by -1%
     		   \PoS@warnl{Floating tables \the\count0\space
            		\space and \the\ftabcount \space colliding}}\fi
 \iffigprocessing  \PoS@warnl{Floating figure \the\ffigcount\space and 
			floating table \the\ftabcount\space colliding}\fi
 \postpone@captions
 \global\setbox\@tmpbox=\hbox\bgroup% begin of tabbox, ACTUALLY AN HBOX.
}
{%
\egroup% end of \tabbox, 		% ACTUALLY \@tmpbox
\global\setlength{\floatfltwidth}{\the\wd\@tmpbox}%
\ifdim\floatfltwidth<.6\columnwidth%	% IF NOT TOO WIDE OK:
  \global\setbox\tabbox=\vbox{\hsize=\floatfltwidth
			     \def\@captype{table}%
			     \noindent\unhbox\@tmpbox
			     \later@capt\later@label}%
  \global\tabprocessingtrue
  \global\everypar={\trytab\oldeverypar}%
  \tabinsert\par%
\else%					% ELSE NORMAL LATEX TABLE.
%  \PoS@mess{Floating table \the\ftabcount\space is 
%			wide becomes a LaTeX float}%
  \ifdim\floatfltwidth<1.1\columnwidth
    \if\@tside v \def\@tside{ht}\fi
    \def\@@tempa{\@float{table}[}\let\@@tempb\end@float
  \else
    \if\@tside v \def\@tside{t}\fi
    \def\@@tempa{\@dblfloat{table}[}\let\@@tempb\end@dblfloat
  \fi
  \expandafter\@@tempa\@tside]%		% ACTUALLY CALL THE LATEX FLOAT
  \center\unhbox\@tmpbox\later@capt\later@label
  \@@tempb
\fi%
}
%----------- prepare for tries ----------------------------%
\def\figinsert{%
   \global\nosuccesstryfig=0%
   \global\outputpretest={\do@test}%
   \global\tryingfigtrue \global\doingfigfalse%
   \global\pageafterfigfalse}%
\def\tabinsert{%
   \global\nosuccesstrytab=0%
   \global\outputpretest={\do@test}%
   \global\tryingtabtrue \global\doingtabfalse%
   \global\pageaftertabfalse}%
%----------- init -----------------------------------------%
\AtBeginDocument{%
 \edef\oldoutput{\the\output}
 \output={\the\outputpretest\ifoutput\oldoutput\fi}%
 \outputpretest={\outputtrue}%
 \edef\oldeverypar{\the\everypar}% PERCHE' NESSUNO LO RIMETTE PIU' A POSTO?***
}
%---- tests during \outputpretest -------------------------%
\def\do@test{%
   \ifnum\outputpenalty=-10005
%	\output
     \setbox\pagebox=\vbox{\unvbox255}%
     \global\pageht=\ht\pagebox
     \global\outputfalse
     \unvbox\pagebox
     \dimen0=\pageht\advance\dimen0 by 2 \baselineskip
     \ifdim\dimen0>\vsize\global\pageht\z@\fi
   \else
     \global\outputtrue
     \ifnum\outputpenalty>-\@Mi			%ie \marginpar's give penalty.
	\ifdoingtab\global\pageaftertabtrue\fi
	\ifdoingfig\global\pageafterfigtrue\fi
     \fi
   \fi}%
%-------- check for side ----------------------------------%
\def\chk@side#1{% DEFINED \fl@sid, BUT IF CHOSEN A DEFAULT, ELIMINATE ALL ***
	{\edef\fl@sid{#1}%
	  \if\fl@sid r\global\oddpagestrue\fi
          \if\fl@sid l\global\oddpagesfalse\fi
          \if\fl@sid p%
              \ifodd\c@page\global\oddpagesfalse
                \else\global\oddpagestrue\fi
          \fi
          \if\fl@sid v%
             \ifnum\theOptionTest=0%
                 \ifodd\c@page\global\oddpagesfalse
                   \else\global\oddpagestrue\fi
             \else
                  \ifodd\theOptionTest\global\oddpagestrue
                    \else\global\oddpagesfalse\fi
             \fi
          \fi
         }}%
%-------- get point in page -------------------------------%
\def\get@pageht{{\everypar={\relax}\setbox0=\lastbox
       		\parindent=\wd0 \parskip=\z@ \par
         	\penalty-10005 \leavevmode}}%
%-------- try at everypar ---------------------------------%
\def\tryfig{%
	\global\fl@ht\ht\figbox
	\global\fl@wd\wd\figbox
	\global\let\fl@gut\figgutter
      \iftryingfig
	\get@pageht
	\dimen0=\vsize
	\advance\dimen0 by -\pageht
	\advance\dimen0 by -2\baselineskip
	\ifdim\dimen0>\fl@ht
	  \chk@side\@fside
	  \dimen0=0.3\baselineskip
	  \vrule depth \dimen0 width \z@%height 1.5\baselineskip
	  \vadjust{\kern -\dimen0
	  \vtop to \dimen0{%
             \baselineskip=\dimen0
             \vss \vbox to 1ex{%
             \ifoddpages
               \hb@xt@\hsize{\hss\copy\figbox}%
             \else% leftsetting
               \hb@xt@\hsize{\copy\figbox\hss}%
             \fi
             \vss}\null}}%
	  \global\tryingfigfalse
	  \global\doingfigtrue
	  \global\startpageht=\pageht
	  \global\htdone=\z@
	  \dohang
	  \ifnum\nosuccesstryfig>0%
	   \PoS@mess{Flt. fig. \the\ffigcount\space set on page \the\count0, 
	 		shifted \the\nosuccesstryfig\space par(s) forward}%
%	  \else
%	   \PoS@mess{Floating figure \the\ffigcount\space 
%			set on page \the\count0}%
	  \fi
	\else
          \global\advance\nosuccesstryfig by 1
	\fi
      \else%				% IF NOT TRYING
	\ifdoingfig
	  \get@pageht
	  \global\htdone=\pageht
	  \global\advance\htdone by -\startpageht
	  \ifpageafterfig
	    \global\doingfigfalse% IN CASE A PAGEBREAK JUST BELOW?
	  \else
	    \dimen0=\fl@ht
	    \advance\dimen0 by .85\baselineskip	% .85 ENOUGH: WE ARE AT NEW PAR
%	    \typeout{FLOAT HT: \the\dimen0, HTDONE: \the\htdone}
	    \ifdim\htdone<\dimen0%
		\dohang
	    \else
		\global\doingfigfalse
	    \fi
	  \fi
	  \ifdoingfig\else\global\figprocessingfalse\fi% IF NO MORE DOING.
	\else
	  \global\outputpretest={\outputtrue}% NOT RESTORE EVPAR IF NOT DOING?
	\fi%			% END IF DOING
      \fi%			% END IF TRYING
}
\def\trytab{%
	\global\fl@ht\ht\tabbox
	\global\fl@wd\wd\tabbox
	\global\let\fl@gut\tabgutter
       \iftryingtab
	\get@pageht
       \dimen0=\vsize
       \advance\dimen0 by -\pageht
       \advance\dimen0 by -2\baselineskip
       \ifdim\dimen0>\fl@ht
	 \chk@side\@tside
         \dimen0=0.3\baselineskip
         \vrule depth \dimen0 width \z@
         \vadjust{\kern -\dimen0
         \vtop to \dimen0{%
             \baselineskip=\dimen0
             \vss \vbox to 1ex{%
             \ifoddpages
               \hb@xt@\hsize{\hss\copy\tabbox}%
             \else% leftsetting
               \hb@xt@\hsize{\copy\tabbox\hss}%
             \fi
             \vss}\null}}%
       \global\tryingtabfalse
       \global\doingtabtrue
       \global\startpageht=\pageht
       \global\htdone=\z@
       \dohang
       \ifnum\nosuccesstrytab>0%
	\PoS@mess{Flt. tab. \the\ftabcount\space set on page \the\count0, 
			shifted \the\nosuccesstrytab\space par(s) forward}%
       \fi
     \else
       \global\advance\nosuccesstrytab by 1
     \fi
     \else
     \ifdoingtab
       \get@pageht
    \global\htdone=\pageht
    \global\advance\htdone by -\startpageht
    \ifpageaftertab
      \global\doingtabfalse
    \else
      \dimen0=\fl@ht
      \advance\dimen0 by .85\baselineskip	% .85 ENOUGH: WE ARE AT NEW PAR
      \ifdim\htdone<\dimen0%
         \dohang
      \else
         \global\doingtabfalse
      \fi
    \fi
    \ifdoingtab\relax\else\global\tabprocessingfalse\fi
  \else
    \global\outputpretest={\outputtrue}%
  \fi
\fi
}
%----- hanging lord -----------------------------%
\def\dohang{%
   \dimen0=\fl@ht
   \advance\dimen0 by -\htdone
   \advance\dimen0 by 1.999\baselineskip	% (16/12/98) WAS 1.49 BUT
   \hangcount=\dimen0				% TEX ROUNDS ON STRICT INTEGERS
   \divide\hangcount by \baselineskip		% IN THIS DIVISION!
     \dimen0=\fl@wd
     \advance\dimen0 by \fl@gut
     \ifoddpages
       \global\hangindent=-\dimen0% placing right
     \else 
       \global\hangindent=\dimen0% placing left
     \fi
   \global\hangafter=-\hangcount
%   \fi
}
%----------------------- TWO NEWITEMS ---------------------%
\newcommand{\fltitem}[2][\z@]{\setlength{\fltitemwidth}{\linewidth}%
  \addtolength{\fltitemwidth}{-\floatfltwidth}%
  \addtolength{\fltitemwidth}{-0.5em}%
  \item \parbox[t]{\fltitemwidth}{#2}\\[#1]}
\newcommand{\fltditem}[3][\z@]{\setlength{\fltitemwidth}{\linewidth}%
  \addtolength{\fltitemwidth}{-\floatfltwidth}%
  \addtolength{\fltitemwidth}{-0.5em}%
  \item[#2] \parbox[t]{\fltitemwidth}{#3}\\[#1]}
%%%%%%\endinput
%% 
%% End of ex-file `floatflt.sty'.

%%% this is a modification to the standard latex to make bottom
%%% figures appear above the footnote
% problema: se nella stessa pagina ci sono una figura piccola e una
% [b] o [t] dopo di essa quella piccola viene a sinistra
% ma questo succede anche senza questa patch!
% e con o senza footnote.

\newbox\JCAP@outputbox
\gdef \@makecol {%
   \ifvoid\footins
     \setbox\@outputbox \box\@cclv
   \else
     \setbox\@outputbox \vbox {%
       \boxmaxdepth \@maxdepth
       \@tempdima\dp\@cclv
	\unvbox \@outputbox
       \unvbox \@cclv
       \vskip-\@tempdima
     }%
     \setbox\JCAP@outputbox \vbox {%
       \vskip \skip\footins
       \color@begingroup
         \normalcolor
         \footnoterule
         \unvbox \footins
       \color@endgroup
       }%
   \fi
   \xdef\@freelist{\@freelist\@midlist}%
   \global \let \@midlist \@empty
   \@combinefloats
   \ifvbox\@kludgeins
     \@makespecialcolbox
   \else
     \setbox\@outputbox \vbox to\@colht {%
       \@texttop
       \dimen@ \dp\@outputbox
       \unvbox \@outputbox
       \vskip -\dimen@
       \unvbox\JCAP@outputbox
       \@textbottom
       }%
   \fi
   \global \maxdepth \@maxdepth
}
%%% end of the patch

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% CAPTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\newlength\abovecaptionskip
\newlength\belowcaptionskip
\setlength\abovecaptionskip{8\p@ plus 1pt minus 2pt}
\setlength\belowcaptionskip{0\p@}
\long\def\@makecaption#1#2{%
  \if@hyper{\edef\@pippo{the\@captype}
	    \name{\@captype\expandafter\csname\@pippo\endcsname}}\fi%
  \vskip\abovecaptionskip
 {\let\label\@gobble%				% FN 10.2.97 REMOVED LATEX BUG:
  \let\index\@gobble%				% LARGE CAPTIONS PROCESS LABEL
  \let\glossary\@gobble%			% TWO TIMES.
  \sbox\@tempboxa{\small {\bfseries #1:} #2}%	% 
  \global\dimen0\wd\@tempboxa}%			%
  \ifdim \dimen0 >\hsize
    \small {\bfseries #1:} #2\par
  \else
    \global\@minipagefalse \sbox\@tempboxa{\small {\bfseries #1:} #2}%
    \hb@xt@\hsize{\hfil\box\@tempboxa\hfil}%
  \fi
  \vskip\belowcaptionskip}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% FONTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\DeclareOldFontCommand{\rm}{\normalfont\rmfamily}{\mathrm}
\DeclareOldFontCommand{\sf}{\normalfont\sffamily}{\mathsf}
\DeclareOldFontCommand{\tt}{\normalfont\ttfamily}{\mathtt}
\DeclareOldFontCommand{\bf}{\normalfont\bfseries}{\mathbf}
\DeclareOldFontCommand{\it}{\normalfont\itshape}{\mathit}
\DeclareOldFontCommand{\sl}{\normalfont\slshape}{\@nomath\sl}
\DeclareOldFontCommand{\sc}{\normalfont\scshape}{\@nomath\sc}
\DeclareRobustCommand*\cal{\@fontswitch\relax\mathcal}
\DeclareRobustCommand*\mit{\@fontswitch\relax\mathnormal}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% TOC %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\newcommand\tocsecs{}
\newcommand\@pnumwidth{1.55em}
\newcommand\@tocrmarg{2.55em}
\newcommand\@dotsep{4.5}
\setcounter{tocdepth}{3}
\newcommand\tableofcontents{%
\if@todotoc
    \section*{\contentsname\label{-TOC-}}
    \@starttoc{toc}%
    \gdef\tableofcontents{\PoS@igno{\tableofcontents\space already done}}
\else
\PoS@ignol{\tableofcontents. Use option [toc]}
\fi}
\newcommand*\l@part[2]{\def\hyp@typ{part}%	
  \ifnum \c@tocdepth >-2\relax
    \addpenalty\@secpenalty
    \addvspace{2.25em \@plus\p@}%
    \begingroup
      \setlength\@tempdima{3em}%
      \parindent \z@ \rightskip \@pnumwidth
      \parfillskip -\@pnumwidth
      {\leavevmode
       \large \bfseries \tocsecs#1\hfil \hb@xt@\@pnumwidth{\hss
		\intPlink{\hash pag#2}{#2}}}\par
       \nobreak
       \if@compatibility
         \global\@nobreaktrue
         \everypar{\global\@nobreakfalse\everypar{}}%
      \fi
    \endgroup
  \fi}
\newcommand*\l@section[2]{\def\hyp@typ{sec}%
  \ifnum \c@tocdepth >\z@
    \addpenalty\@secpenalty
    \addvspace{1.0em \@plus\p@}%
    \setlength\@tempdima{1.8em}%	%WIDTH OF SECT. N.
    \begingroup
      \let\d@t.%
      \parindent \z@ \rightskip \@pnumwidth
      \parfillskip -\@pnumwidth
      \leavevmode \bfseries
      \advance\leftskip\@tempdima
      \hskip -\leftskip
        \tocsecs#1\nobreak\hfil %HREF HERE WOULD GET ALSO THE TITLE(...)
        \nobreak\hb@xt@\@pnumwidth{\hss \intPlink{\hash pag#2}{#2}}\par
    \endgroup
  \fi}

\newcommand*\l@subsection{\def\hyp@typ{sec}\@tocline{2}{1.8em}{2.3em}}
\newcommand*\l@subsubsection{\def\hyp@typ{sec}\@tocline{3}{4.1em}{3.1em}}
\newcommand*\l@paragraph{\def\hyp@typ{sec}\@tocline{4}{7.2em}{4.3em}}
\newcommand*\l@subparagraph{\def\hyp@typ{sec}\@tocline{5}{10.5em}{5em}}

\if@hyper
  \let\old@dtl\@dottedtocline
  \def\@dottedtocline#1#2#3#4#5{\old@dtl{#1}{#2}{#3}{#4}{%
						\intPlink{\hash pag#5}{#5}}}
\fi

\def\@tocline#1#2#3#4#5{%
  \ifnum #1>\c@tocdepth \else
    \vskip \z@ \@plus.2\p@
    {\leftskip #2\relax \rightskip \@tocrmarg \parfillskip -\rightskip
     \parindent #2\relax\@afterindenttrue
     \interlinepenalty\@M
     \leavevmode
     \@tempdima #3\relax
     \advance\leftskip \@tempdima \null\nobreak\hskip -\leftskip
     {#4}\nobreak
     \hfill
     \nobreak
     \hb@xt@\@pnumwidth{\hfil\normalfont \normalcolor \intPlink{\hash pag#5}{#5}}%
     \par}%
  \fi}


%%%%%%%%%%%%%%%%%%%%%% LIST OF FIGURES AND TABLES %%%%%%%%%%%%%%%%%%%%%
\if@draft			% ONLY IF DRAFT! ... 
 \newcommand\listoffigures{%
     \section*{\listfigurename
       \@mkboth{\MakeUppercase\listfigurename}%
               {\MakeUppercase\listfigurename}}%
     \@starttoc{lof}%
     }
 \newcommand*\l@figure{\def\hyp@typ{figure}\@dottedtocline{1}{1.5em}{2.3em}}
 \newcommand\listoftables{%
     \section*{\listtablename
       \@mkboth{%
           \MakeUppercase\listtablename}{\MakeUppercase\listtablename}}%
     \@starttoc{lot}%
     }
 \newcommand*\l@table{\def\hyp@typ{table}\@dottedtocline{1}{1.5em}{2.3em}}
\else
 \newcommand\listoffigures{\PoS@ignol{List of figures (draft mode only)}}
 \newcommand\listoftables{\PoS@ignol{List of tables (draft mode only)}}
\fi
%%%%%%%%%%%%%%%%%%%%%%%%%%%%% BIBLIOGRAPHY %%%%%%%%%%%%%%%%%%%%%%%%%%%%***
\newdimen\bibindent
\setlength\bibindent{1.5em}
\newenvironment{thebibliography}[1]%	%UNFORTUNATELY MODIFIED..
     {\bgroup\raggedright\small\section*{\refname
        \@mkboth{\MakeUppercase\refname}{\MakeUppercase\refname}}%
      \list{\name{bib\@arabic\c@enumiv}% HOPE!
	    \@biblabel{\@arabic\c@enumiv}}%
           {\settowidth\labelwidth{\@biblabel{#1}}%
            \leftmargin\labelwidth
            \advance\leftmargin\labelsep
            \@openbib@code
            \usecounter{enumiv}%
            \let\p@enumiv\@empty
            \renewcommand\theenumiv{\@arabic\c@enumiv}}%
      \sloppy\clubpenalty4000\widowpenalty4000%
      \sfcode`\.\@m}
     {\def\@noitemerr
       {\@latex@warning{Empty `thebibliography' environment}}%
      \endlist\egroup}
\newcommand\newblock{\hskip .11em\@plus.33em\@minus.07em}
\if@draft
 \let\old@bbt\@bibitem\let\old@lbbt\@lbibitem% LOOK THE DISASTER HERE BELOW.
 \def\@lbibitem[#1]#2{\old@lbbt[#1]{#2}\reversemarginpar{\sf\bfseries\small#2}}
 \def\@bibitem#1{\old@bbt{#1}\reversemarginpar{\sf\bfseries\small#1}}
\fi
\let\@openbib@code\@empty

\newcommand{\pos}[1]{\href{http://pos.sissa.it/cgi-bin/reader/contribution.cgi?id=#1}{\tt #1}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% FOOTNOTES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\skip\footins = 1\bigskipamount plus 2pt minus 4pt

\addtolength{\footnotesep}{1pt}
\renewcommand\footnoterule{%	% NO HYPER...
  \kern-3\p@
  \hrule\@width.4\columnwidth
  \kern2.6\p@}
\newcommand\@makefntext[1]{%
    \parindent 20.39995pt%
%\setlength\parindent{20pt} 
   \noindent
    \hb@xt@\parindent{\hss\@makefnmark}#1}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% NAMES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\newcommand\contentsname{Contents}
\if@draft\newcommand\listfigurename{List of figures}
	 \newcommand\listtablename{List of tables}\fi
\newcommand\refname{References}
\newcommand\indexname{Index}
\newcommand\figurename{Figure}
\newcommand\tablename{Table}
\newcommand\partname{Part}
\newcommand\appendixname{Appendix}
\newcommand\abstractname{Abstract:}
\newcommand\acknowlname{Acknowledgments}

\newcommand\today{}
\edef\today{\ifcase\month\or
  January\or February\or March\or April\or May\or June\or
  July\or August\or September\or October\or November\or December\fi
  \space\number\day, \number\year}

\setlength\columnsep{20\p@}
\setlength\columnseprule{0\p@}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% DEFAULTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\pagestyle{PoS}
\pagenumbering{arabic}
\@addtoreset{equation}{section}
		\let\pagestyle\@gobble
		\let\thispagestyle\@gobble
		\let\pagenumbering\@gobble
\onecolumn
						
% FOR REQUESTED FRONTMATTER ...
\AtBeginDocument{\bgroup%		SOME NICE (LOCAL) MESSAGES... 
	\def\@APPwarn#1{\PoS@warnl{\protect#1 should appear 
					before \protect\begin{document}}}%
	\def\@APPerr#1{\ClassError{PoS}{\protect#1 should appear 
					before \protect\begin{document}}}%
	\if@author\else\@APPerr{Some \protect\author{...}}\fi
	\if@abstract\else\@APPerr{\abstract{...}}\fi
	\if@conf\else\@APPerr{\FullConference{...}}\fi
	\if@shorttitle\else\@APPerr{\ShortTitle{...}}\fi
	\egroup
	\auto@maketitle
	\if@speaker\else\PoS@warnl{One \protect\speaker{...}
			must be specified among authors}\fi
			\if@dblspeak\PoS@warnl{A single \protect\speaker{...}
			should appear in proceedings}\fi
}
\endinput
%% 
%% End of file `PoS.cls'.

### Script makes git commit easier by handling directory locations, .gitignore, large file commit prevention, and input simplification.

##need to run "git config" first
## e.g. "git config credential.https://example.com.username myusername"
## or "git config --global credential.helper osxkeychain"

### TODO: Check if git pull / merge / stash /pop is necessary before trying to push




if [[ "$OSTYPE" == "linux-gnu" ]]; then
# ...
    git config --global credential.helper store
elif [[ "$OSTYPE" == "darwin"* ]]; then
# Mac OSX
    git config --global credential.helper osxkeychain
elif [[ "$OSTYPE" == "cygwin" ]]; then
# POSIX compatibility layer and Linux environment emulation for Windows
    echo "need to modify script to work for cygwin"
elif [[ "$OSTYPE" == "msys" ]]; then
# Lightweight shell and GNU utilities compiled for Windows (part of MinGW)
    echo "need to modify script to work for msys"
elif [[ "$OSTYPE" == "win32" ]]; then
# I'm not sure this can happen.
    echo "need to modify script to work for win32"
elif [[ "$OSTYPE" == "freebsd"* ]]; then
# ...
    echo "need to modify script to work for freebsd"
else
# Unknown.
    echo "unknown OS"
fi

BASEDIR=$(dirname "$0")
export LOCAL_GIT_DIR=$BASEDIR'/..'
echo -e "\ngit dir: "$LOCAL_GIT_DIR
export ORIG_DIR=$(pwd)
cd $LOCAL_GIT_DIR
echo -e "\n $(git status)"


echo "updating Dene-Farrell-Insight code on github"
echo "local git folder: "$LOCAL_GIT_DIR
GIT_ORIGIN="$(git config --get remote.origin.url)"
echo "origin to which script pushes: "$GIT_ORIGIN


if [[ "$OSTYPE" == "darwin"* ]]; then
    echo -e "\n (0) Dealing with pain-in-the-ass .DS_Store Files  (0)"
    ### Remove .DS_Store from git cache
    git rm --cached .DS_Store
    find . -name .DS_Store -print0 | xargs -0 git rm --ignore-unmatch
fi

### Also create a global ignore and add .DS_Store, unless it's already been done
if grep -Fxq ".DS_Store" ~/.gitignore_global
then
    echo ".DS_Store already found in preexisting ~/.gitignore_global File"
else
    echo "adding .DS_Store to ~/.gitignore_global File and applying to all dirs"
    echo .DS_Store >> ~/.gitignore_global
    git config --global core.excludesfile ~/.gitignore_global
fi


echo -e "\n (1) Showing 45+MB Files - Adding Big Files to .gitignore:  (1)"
echo "$(find . -size +45M)"
# for some reason .gitignore doesn't recognize files with './' before their name
# perhaps git references some other directory than it's own location (?)
find . -size +45M | sed "s/\.\///g" | cat >> .gitignore
find . -size +45M | cat >> .gitignore
echo "*.DS_Store" >> .gitignore
echo ".DS_Store" >> .gitignore
echo ".tmpstore" >> .gitignore

### Input folders are too large to efficiently manage with git
git rm -r --cached input
echo "input/" >> .gitignore

#remove duplicates in .gitignore created by running this script
### 'tee' is silent without an 'echo' immediately after (don't know why)
### 'tee' function is behaving sketchily -- sometimes outputs the unique list, sometimes nothing.
#echo -e "\n Showing .gitignore contents [with tee]:"
#sort .gitignore | uniq | tee .gitignore

### apparently 'uniq' cannot write (pipe) to the same file that it is reading with '>'
### solution was to write to second file, then transfer back to the original, overwriting it
echo -e "\n Showing .gitignore contents [after unique]:"
sort .gitignore | uniq > .tmpstore
cat .tmpstore | cat > .gitignore
rm .tmpstore
echo "$(cat .gitignore)"
echo -e "\n "


## Check for large files above 100MB / 50MB
echo -e '\n (2) checking for large files [50+MB] (2)'
NUM_BIG_FILES=$(find $LOCAL_GIT_DIR -type f -not -path "/*.git/*" -size +50000k -exec ls -lh {} \; | wc -l)



## Warn User if big files exist
if [[ $NUM_BIG_FILES -ne 0 ]] ; then
echo -e "\nRemove or ignore big files before running git."
find $LOCAL_GIT_DIR -type f -not -path '/*.git/*' -size +50000k -exec ls -lh {} \; | awk '{printf "%s %s \n", $5,$9}' | cat
echo -e "\n $(git status)"
echo -e "\nDouble check that large files are not being tracked and pushed!"
    read -p "Continue with commit?" -n 1 -r
    echo ' ' # move to a new line
    if [[ ! $REPLY =~ ^[Yy]$ ]]
    then
        exit 1
    fi
fi



## Prep commit message and push changes otherwise

GENERIC_END_TAG='GENERIC-COMMIT-all-changes'
echo -e '\n (3) commit (3)'
echo "input the commit message (no quotes), followed by [ENTER]:"
read USR_END_TAG
echo 'commit msg = '"$USR_END_TAG"


if [ ! -z "$USR_END_TAG" ] ; then
echo "using commit msg"
COM_MSG=$USR_END_TAG
else
echo "using generic msg - commit msg empty"
COM_MSG=$GENERIC_END_TAG
fi

cd $LOCAL_GIT_DIR
echo "currenlty in: "$(pwd)
echo '---status before git add: ---'
git status -s
git add .
echo '---status after git add: ---'
git status -s
GIT_ACTION="commit -a -m \'$COM_MSG\'"

echo '---comitting---'
git commit -a -m "$COM_MSG"

echo -e '\n (4) push (4)'
echo '---pushing---'
git push
cd $ORIG_DIR
echo '---finished---'

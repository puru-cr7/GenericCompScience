var SHA256 = require("./crypto-js/sha256");

class Block{

	constructor(index,timestamp,data,previousHash){
		this.index=index;
		this.timestamp=timestamp;
		this.data=data;
		this.previousHash=previousHash;
		this.hash=this.calculateHash();
	}

	calculateHash(){
		return SHA256(this.index + this.timestamp + JSON.stringify(this.data) + this.previousHash).toString();
	}
}

class BlockChain{
	constructor(){
		this.chain=[this.createGenesisBlock()];
	}

	createGenesisBlock(){
		return new Block(0,"29/12/2017","Genesis block","0");
	}

	getLatestBlock(){
		return this.chain[this.chain.length-1];
	}

	addBlock(newBlock){
		 newBlock.previousHash=this.getLatestBlock().hash;
		 newBlock.hash=newBlock.calculateHash();
		 this.chain.push(newBlock);
	}

	isChainValid(){
		for(var i=1;i<this.chain.length;i++){
			const currentBlock=this.chain[i];
			const prevBlock=this.chain[i-1];
			if(currentBlock.hash!=currentBlock.calculateHash()){
				return false;
			}

			if(currentBlock.previousHash!=prevBlock.hash){
				return false;
			}
		}
		return true;
	}
}

var dpCoin=new BlockChain();
dpCoin.addBlock(new Block(1,"30/12/2017",{amount:4}));
dpCoin.addBlock(new Block(2,"31/12/2017",{amount:10}));

console.log(JSON.stringify(dpCoin,null,4));
console.log(dpCoin.isChainValid()?"valid chain":"invalid chain");